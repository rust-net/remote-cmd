use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::response::Response;
use futures::SinkExt;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

use crate::server::auth::ClientAuth;
use crate::server::state::AppState;

/// 单文件上传上限
const MAX_UPLOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// GET /api/upload/ws：WS 连接即单文件上传会话（upgrade 前经 ClientAuth 认证）。
/// 协议：init 文本帧（name/size/session）→ Binary 数据帧×N → upload_end 文本帧
/// → 应答 uploaded（path = 落地绝对路径）/ rejected 文本帧。
/// 客户端由 PTY 会话的拖放/粘贴路径检测触发（见 client/run_cmd.rs），文件落地
/// Unix 时优先所传会话 shell 进程 cwd、否则数据目录 uploads/ 下，同名自动加 _N 后缀，不做覆盖。
pub async fn upload_ws(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ClientAuth(client): ClientAuth,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| session(socket, state, client, addr))
}

#[derive(Deserialize)]
struct UploadInit {
    /// 文件名；mkdir 模式下可为空
    #[serde(default)]
    name: String,
    /// 发起上传的 exec 会话 id：服务端据此定位该会话 shell 进程 cwd 作为落地目录
    #[serde(default)]
    session: String,
    /// 目录上传的相对路径（POSIX `/` 分隔，逐段净化防穿越）；单文件上传为空
    #[serde(default)]
    rel_path: String,
    /// 批次级断点续传：true 且会话清单已含该文件 → 应答 exists（浏览器跳过）
    #[serde(default)]
    resume: bool,
    /// 空目录还原：只建目录不收文件，应答 mkdir_ok / dir_exists（probe 模式）
    #[serde(default)]
    mkdir: bool,
    /// mkdir 模式子选项：目录已存在时仅探测不创建，应答 dir_exists
    #[serde(default)]
    probe: bool,
    /// mkdir 模式子选项：目录已存在时自动改名为 root_N 创建（应答带实际 rel 名）
    #[serde(default)]
    auto_rename: bool,
    /// 文件冲突语义：true = 同名文件覆盖写（目录上传合并语义）；
    /// false = 同名自动 _N 重命名（单文件上传默认）
    #[serde(default)]
    overwrite: bool,
    /// 目录批次的钉定基准目录（绝对路径）：批次开始时由首次探测应答的 base 回传，
    /// 整批固定落盘位置——避免 shell 上传期间 cd 导致文件漂移，也免去逐文件重跑
    /// cwd 解析链。空 = 逐请求解析（旧行为）
    #[serde(default)]
    base_dir: String,
}

/// 净化目录相对路径：按 `/` 分段，逐段走文件名净化（拒绝 `.`/`..`/控制字符），
/// 空段折叠；段数 ≤16、总长 ≤512。空串合法（落地在根目录）。非法返回 None。
fn sanitize_rel_path(raw: &str) -> Option<String> {
    if raw.is_empty() {
        return Some(String::new());
    }
    let mut comps = Vec::new();
    for seg in raw.split('/') {
        if seg.is_empty() {
            continue; // 折叠 "a//b"、首尾 '/'
        }
        if seg.contains('\\') {
            return None; // 目录段不允许反斜杠（Windows 分隔符混淆，防被净化函数截断成意外段）
        }
        comps.push(sanitize_name(seg)?);
    }
    if comps.len() > 16 || comps.iter().map(|c| c.len() + 1).sum::<usize>() > 512 {
        return None;
    }
    Some(comps.join("/"))
}

fn rejected(reason: &str) -> Message {
    Message::Text(serde_json::json!({ "type": "rejected", "reason": reason }).to_string().into())
}

/// 上传落地目标：宿主目录或容器内目录（docker exec 会话，文件经本地暂存后推入容器）
enum UploadTarget {
    Host(PathBuf),
    Container {
        /// 容器目标（id/ref/容器内 cwd 基准）
        ctr: crate::common::docker::ContainerCwd,
        /// 本地暂存根目录：接收帧先落盘，完成后推入容器（复用既有接收/续传/重命名逻辑）
        staging: PathBuf,
    },
}

impl UploadTarget {
    /// 本地暂存/落盘根目录（容器目标即 staging）
    fn local_base(&self) -> &std::path::Path {
        match self {
            UploadTarget::Host(p) => p,
            UploadTarget::Container { staging, .. } => staging,
        }
    }
}

/// 上传落地目标解析：Unix 经 cwd 解析链取所传 exec 会话（shell）进程的 cwd
/// （tmux 感知：tmux 内 cd 后取活动 pane 实时 cwd，见 common/cwd.rs；docker exec
/// 会话直接写容器内，见 common/docker.rs；拖放上传语义 = 文件落在 shell 当前工作
/// 目录）；会话 id 缺失/未知/归属客户端不符、解析失败或 Windows 服务端 → 默认
/// uploads 目录
async fn upload_target(state: &AppState, client_id: &str, session: Option<&str>) -> UploadTarget {
    let default = || UploadTarget::Host(state.data_dir.join("uploads"));
    let pid = session.and_then(|s| state.pty_pid(s, client_id));
    let Some(pid) = pid else {
        macro_log::d!("[upload] 回退默认目录: session={session:?} 无已登记 pid（会话未登记/已结束/客户端未回传 session）");
        return default();
    };
    #[cfg(unix)]
    if let Some(target) = crate::common::cwd::resolve_process_cwd(pid).await {
        return match target {
            crate::common::cwd::ProcessCwd::Host(dir) => UploadTarget::Host(PathBuf::from(dir)),
            crate::common::cwd::ProcessCwd::Container(ctr) => {
                // 容器会话：本地暂存目录按会话唯一，接收完成后逐文件推入容器
                let staging = state.data_dir.join("uploads").join(format!(
                    "ctr-{}-{}",
                    session.unwrap_or_default(),
                    chrono::Utc::now().timestamp_millis()
                ));
                macro_log::d!(
                    "[upload] 容器落地目标: ctr={} cwd={} 暂存={}",
                    ctr.container_ref,
                    ctr.container_cwd,
                    staging.display()
                );
                UploadTarget::Container { ctr, staging }
            }
        };
    }
    #[cfg(not(unix))]
    let _ = pid;
    macro_log::d!("[upload] 回退默认目录: pid={pid} cwd 解析链失败（见 [cwd] 日志）");
    default()
}

async fn session(
    mut socket: WebSocket,
    state: Arc<AppState>,
    client: crate::server::store::ClientRecord,
    addr: SocketAddr,
) {
    let hostname = client.hostname.clone();

    // init 帧（15s 超时）
    let init = match tokio::time::timeout(Duration::from_secs(15), read_init(&mut socket)).await {
        Ok(Ok(v)) => v,
        Ok(Err(reason)) => {
            let _ = socket.send(rejected(&reason)).await;
            let _ = socket.close().await;
            return;
        }
        Err(_) => {
            let _ = socket.send(rejected("等待 init 帧超时")).await;
            let _ = socket.close().await;
            return;
        }
    };

    // 目录相对路径净化（目录上传的层级还原；单文件上传为空）
    let Some(rel) = sanitize_rel_path(&init.rel_path) else {
        let _ = socket.send(rejected("目录路径非法")).await;
        let _ = socket.close().await;
        return;
    };

    // 落地基准：base_dir 只能是服务端首次解析并钉定的值（会话内严格相等校验，
    // 客户端仅可回声、不可伪造任意绝对路径——上传通道不得成为独立于 shell 的任意
    // 写原语）；未携带 base_dir 时逐请求解析（Unix 优先会话 shell cwd / tmux 感知 /
    // docker exec 容器内 cwd，Windows → uploads）
    let session_owned = init.session.clone();
    let session = (!session_owned.is_empty()).then_some(session_owned.as_str());
    let target = if !init.base_dir.is_empty() {
        let pinned = session.and_then(|s| state.upload_base(s));
        let valid = pinned.as_deref() == Some(init.base_dir.as_str())
            && (PathBuf::from(&init.base_dir).is_absolute() || init.base_dir.starts_with('/'));
        if !valid {
            macro_log::w!(
                "[upload] base_dir 与会话钉定值不符，拒绝（可能为伪造请求）: {} (session={:?})",
                init.base_dir,
                session
            );
            state.audit.log(
                "file.upload.reject",
                &hostname,
                Some(serde_json::json!({ "reason": "base_dir_mismatch", "base_dir": init.base_dir, "ip": addr.ip().to_string() })),
            );
            let _ = socket.send(rejected("base_dir 无效（与会话钉定基准不符）")).await;
            let _ = socket.close().await;
            return;
        }
        // 容器目标与会话钉定一并回放（客户端仅回声路径，容器目标始终以服务端记录为准）
        match state.container_target(&session_owned) {
            Some(ctr) => UploadTarget::Container { ctr, staging: PathBuf::from(&init.base_dir) },
            None => UploadTarget::Host(PathBuf::from(&init.base_dir)),
        }
    } else {
        let t = upload_target(&state, &client.id, session).await;
        // 首次解析：钉定会话基准（客户端后续回传此值才被接受）
        state.set_upload_base(&session_owned, &display_path(t.local_base()));
        if let UploadTarget::Container { ctr, .. } = &t {
            state.set_container_target(&session_owned, ctr.clone());
        }
        t
    };
    let base = target.local_base().to_path_buf();
    // 容器会话内 base_dir 对应的实际容器目录（后续 mkdir/推送都以它为基准）
    let ctr_dir = |ctr: &crate::common::docker::ContainerCwd, rel: &str| {
        crate::common::docker::container_join(&ctr.container_cwd, rel)
    };
    let dir = if rel.is_empty() {
        base.clone()
    } else {
        base.join(&rel)
    };
    // 应答的 base 统一取钉定值（客户端回声校验的对象；避免创建前后 canonicalize 差异）
    let base_display = state
        .upload_base(&session_owned)
        .unwrap_or_else(|| display_path(&base));

    // mkdir 模式：空目录还原 / 根目录探测（probe、auto_rename），只建目录不收文件
    if init.mkdir {
        if rel.is_empty() {
            let _ = socket.send(rejected("mkdir 缺少目录路径")).await;
            let _ = socket.close().await;
            return;
        }
        // 存在性探测：宿主查本地 fs；容器 exec test -d（存在性以容器内为准）
        let existed = match &target {
            UploadTarget::Host(_) => tokio::fs::try_exists(&dir).await.unwrap_or(false),
            UploadTarget::Container { ctr, .. } => match crate::common::docker::path_exists(&ctr.container_id, &ctr_dir(ctr, &rel), true).await {
                Ok(v) => v,
                Err(e) => {
                    macro_log::w!("[upload] 容器目录探测失败（视为不存在继续）: {e}");
                    false
                }
            },
        };
        // probe 模式：已存在则仅应答不创建（客户端据此决定覆盖合并或重命名）
        if init.probe && existed {
            let path = match &target {
                UploadTarget::Host(_) => display_path(&dir),
                UploadTarget::Container { ctr, .. } => format!("ctr:{}:{}", ctr.container_ref, ctr_dir(ctr, &rel)),
            };
            let _ = socket
                .send(Message::Text(
                    serde_json::json!({
                        "type": "dir_exists",
                        "path": path,
                        "base": base_display,
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
        // auto_rename 模式：已存在时自动改名为 root_N（目录级重命名 = 新建另一个目录，
        // 而非把内部每个重名文件改名）
        let mut actual_rel = rel.clone();
        if init.auto_rename && existed {
            let mut i = 1u32;
            loop {
                actual_rel = format!("{rel}_{i}");
                let free = match &target {
                    UploadTarget::Host(_) => !tokio::fs::try_exists(base.join(&actual_rel)).await.unwrap_or(false),
                    UploadTarget::Container { ctr, .. } => !crate::common::docker::path_exists(&ctr.container_id, &ctr_dir(ctr, &actual_rel), true).await.unwrap_or(true),
                };
                if free {
                    break;
                }
                i += 1;
            }
        }
        let mkdir_local = if actual_rel == rel { dir.clone() } else { base.join(&actual_rel) };
        let result = match &target {
            UploadTarget::Host(_) => tokio::fs::create_dir_all(&mkdir_local)
                .await
                .map(|_| display_path(&mkdir_local))
                .map_err(|e| e.to_string()),
            UploadTarget::Container { ctr, .. } => {
                crate::common::docker::mkdir_p(&ctr.container_id, &ctr_dir(ctr, &actual_rel))
                    .await
                    .map(|_| format!("ctr:{}:{}", ctr.container_ref, ctr_dir(ctr, &actual_rel)))
                    .map_err(|e| e.to_string())
            }
        };
        match result {
            Ok(path) => {
                macro_log::d!("[upload] 目录就绪 {path} (from {} {})", hostname, addr.ip());
                let _ = socket
                    .send(Message::Text(
                        serde_json::json!({
                            "type": "mkdir_ok",
                            "path": path,
                            "name": actual_rel,
                            "existed": existed,
                            "base": base_display,
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;
                let _ = socket.close().await;
            }
            Err(e) => {
                macro_log::w!("[upload] 创建目录失败: {e}");
                let _ = socket.send(rejected("服务端目录创建失败")).await;
                let _ = socket.close().await;
            }
        }
        return;
    }

    // 文件名净化：仅保留文件名部分，拒绝空/特殊名/控制字符（防路径穿越）
    let Some(name) = sanitize_name(&init.name) else {
        let _ = socket.send(rejected("文件名非法")).await;
        let _ = socket.close().await;
        return;
    };

    // 批次级断点续传：清单命中 → 应答 exists（浏览器跳过该文件）
    let manifest_key = if rel.is_empty() {
        name.clone()
    } else {
        format!("{rel}/{name}")
    };
    if init.resume && state.upload_done(&session_owned, &manifest_key) {
        macro_log::d!("[upload] 断点续传命中，跳过 {manifest_key} (from {})", hostname);
        let _ = socket
            .send(Message::Text(
                serde_json::json!({ "type": "exists", "key": manifest_key }).to_string().into(),
            ))
            .await;
        let _ = socket.close().await;
        return;
    }

    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        macro_log::w!("[upload] 创建上传目录失败 {}: {e}", dir.display());
        let _ = socket.send(rejected("服务端上传目录创建失败")).await;
        let _ = socket.close().await;
        return;
    }
    // 文件创建：overwrite（目录合并语义，同名覆盖写）或 _N 重命名（单文件默认）
    let (dest, mut file) = if init.overwrite {
        let dest = dir.join(&name);
        let f = match tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dest)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                macro_log::w!("[upload] 创建文件失败 {}/{}: {e}", dir.display(), name);
                let _ = socket.send(rejected("服务端文件写入失败")).await;
                let _ = socket.close().await;
                return;
            }
        };
        (dest, f)
    } else {
        match open_unique_file(&dir, &name).await {
            Ok(v) => v,
            Err(e) => {
                macro_log::w!("[upload] 创建文件失败 {}/{}: {e}", dir.display(), name);
                let _ = socket.send(rejected("服务端文件写入失败")).await;
                let _ = socket.close().await;
                return;
            }
        }
    };
    // init 就绪应答：浏览器目录上传引擎据此区分"可发送数据"（ready）与"已存在跳过"（exists）
    let _ = socket
        .send(Message::Text(
            serde_json::json!({ "type": "ready", "key": manifest_key }).to_string().into(),
        ))
        .await;
    macro_log::d!("[upload] 开始接收 {} -> {} (from {} {})", name, dest.display(), hostname, addr.ip());

    // 数据泵：Binary 帧 → 落盘；upload_end 文本帧 → 正常收尾
    let mut total: u64 = 0;
    let mut done = false;
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Binary(data)) => {
                total += data.len() as u64;
                if total > MAX_UPLOAD_BYTES {
                    macro_log::w!("[upload] 上传超限 ({total} bytes): {} from {}", name, hostname);
                    break;
                }
                if file.write_all(&data).await.is_err() {
                    macro_log::w!("[upload] 文件写入失败: {} from {}", name, hostname);
                    break;
                }
            }
            Ok(Message::Text(t)) => {
                let is_end = serde_json::from_str::<serde_json::Value>(&t)
                    .ok()
                    .and_then(|v| v["type"].as_str().map(|s| s == "upload_end"))
                    .unwrap_or(false);
                if is_end {
                    done = true;
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                macro_log::w!("[upload] 连接读错误: {e} (name={name})");
                break;
            }
            _ => {}
        }
    }

    let _ = file.flush().await;
    drop(file);
    if !done {
        // 非正常收尾（断连/超限/写失败）：删除残留部分文件
        let _ = tokio::fs::remove_file(&dest).await;
        let _ = socket.close().await;
        macro_log::d!("[upload] 会话未正常收尾，已清理残留: {}", dest.display());
        return;
    }

    // 容器目标：接收完成后推入容器内目标目录（本地暂存文件用后即删）
    let mut path = display_path(&dest);
    let mut audit_dir = dir.display().to_string();
    if let UploadTarget::Container { ctr, .. } = &target {
        let dest_dir = ctr_dir(ctr, &rel);
        // 冲突语义：overwrite 直接推（daemon 同名覆盖）；否则容器内 _N 探测重命名
        let mut dest_name = name.clone();
        if !init.overwrite {
            let (stem, ext) = match name.rsplit_once('.') {
                Some((s, _e)) if !s.is_empty() => (&name[..s.len()], &name[s.len()..]),
                _ => (name.as_str(), ""),
            };
            let mut i = 1u32;
            loop {
                let probe = crate::common::docker::container_join(&dest_dir, &dest_name);
                match crate::common::docker::path_exists(&ctr.container_id, &probe, false).await {
                    Ok(false) => break,
                    Ok(true) => {
                        dest_name = format!("{stem}_{i}{ext}");
                        i += 1;
                    }
                    Err(e) => {
                        macro_log::w!("[upload] 容器内重名探测失败: {e}");
                        let _ = tokio::fs::remove_file(&dest).await;
                        let _ = socket.send(rejected("容器状态探测失败")).await;
                        let _ = socket.close().await;
                        return;
                    }
                }
            }
        }
        match crate::common::docker::push_file(&ctr.container_id, &dest_dir, &dest_name, &dest).await {
            Ok(_) => {
                path = format!(
                    "ctr:{}:{}",
                    ctr.container_ref,
                    crate::common::docker::container_join(&dest_dir, &dest_name)
                );
                audit_dir = dest_dir;
                // 清理本地暂存（目录非空时保留）
                let _ = tokio::fs::remove_file(&dest).await;
                let _ = tokio::fs::remove_dir(&dir).await;
            }
            Err(e) => {
                macro_log::w!("[upload] 容器推送失败 {}: {e}", dest.display());
                let _ = tokio::fs::remove_file(&dest).await;
                let _ = socket.send(rejected("容器写入失败")).await;
                let _ = socket.close().await;
                return;
            }
        }
    }

    state.upload_mark(&session_owned, &manifest_key);
    state.audit.log(
        "file.upload",
        &hostname,
        Some(serde_json::json!({ "name": name, "bytes": total, "path": path, "dir": audit_dir, "ip": addr.ip().to_string() })),
    );
    let _ = socket
        .send(Message::Text(
            serde_json::json!({ "type": "uploaded", "path": path, "bytes": total }).to_string().into(),
        ))
        .await;
    let _ = socket.close().await;
    macro_log::d!("[upload] 接收完成 {name} ({total} bytes) -> {path} from {hostname}");
}

async fn read_init(socket: &mut WebSocket) -> Result<UploadInit, String> {
    loop {
        match socket.recv().await {
            Some(Ok(Message::Text(t))) => {
                let v: UploadInit = serde_json::from_str(&t).map_err(|e| format!("init 帧无效: {e}"))?;
                return Ok(v);
            }
            Some(Ok(_)) => return Err("首帧必须为 init".into()),
            Some(Err(e)) => return Err(format!("连接错误: {e}")),
            None => return Err("连接已关闭".into()),
        }
    }
}

/// 文件名净化：取最后一节文件名（兼容 / 与 \ 分隔），拒绝空、`.`/`..`、控制字符
fn sanitize_name(raw: &str) -> Option<String> {
    let name = raw.rsplit(['/', '\\']).next().unwrap_or("").trim();
    if name.is_empty() || name == "." || name == ".." {
        return None;
    }
    if name.chars().any(|c| c.is_control()) {
        return None;
    }
    Some(name.to_string())
}

/// 原子创建不覆盖的落地文件：`create_new`（不存在才创建）替代 exists+create 两步
/// 操作，消除同名并发上传的 TOCTOU 竞态（先查后建互相截断）。冲突时加 _N 后缀重试。
fn create_new_opts() -> tokio::fs::OpenOptions {
    let mut o = tokio::fs::OpenOptions::new();
    o.write(true).create_new(true);
    o
}

async fn open_unique_file(dir: &std::path::Path, name: &str) -> std::io::Result<(PathBuf, tokio::fs::File)> {
    // 首选原名
    let dest = dir.join(name);
    match create_new_opts().open(&dest).await {
        Ok(f) => return Ok((dest, f)),
        Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => return Err(e),
        _ => {}
    }
    // 同名冲突：a.txt → a_1.txt、a_2.txt …（create_new 原子抢位，天然并发安全）
    // 隐藏文件（.gitignore）整体视为无扩展名
    let (stem, ext) = match name.rsplit_once('.') {
        Some((stem, _)) if !stem.is_empty() => (&name[..stem.len()], &name[stem.len()..]),
        _ => (name, ""),
    };
    for i in 1..1000u32 {
        let cand = dir.join(format!("{stem}_{i}{ext}"));
        match create_new_opts().open(&cand).await {
            Ok(f) => return Ok((cand, f)),
            Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => return Err(e),
            _ => {}
        }
    }
    // 兜底：时间戳名（1000 个后缀占满的极端场景）
    let cand = dir.join(format!("{stem}_{}{ext}", chrono::Utc::now().timestamp_millis()));
    let f = create_new_opts().open(&cand).await?;
    Ok((cand, f))
}

/// 落地路径展示：canonicalize 取绝对路径；剥离 Windows `\\?\` 前缀
fn display_path(p: &std::path::Path) -> String {
    let s = std::fs::canonicalize(p)
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_else(|_| p.to_string_lossy().into_owned());
    s.strip_prefix(r"\\?\").map(|s| s.to_string()).unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 目录相对路径净化：空串合法（根目录）、空段折叠、`..`/`.`/控制字符拒绝、
    /// 段数与总长上限
    #[test]
    fn sanitize_rel_path_cases() {
        assert_eq!(sanitize_rel_path(""), Some(String::new()));
        assert_eq!(sanitize_rel_path("docs/sub"), Some("docs/sub".into()));
        // 首尾 '/' 与空段折叠
        assert_eq!(sanitize_rel_path("/docs//sub/"), Some("docs/sub".into()));
        // 防穿越：.. 与 . 整段拒绝
        assert_eq!(sanitize_rel_path("../etc"), None);
        assert_eq!(sanitize_rel_path("docs/../../etc"), None);
        assert_eq!(sanitize_rel_path("docs/./sub"), None);
        assert_eq!(sanitize_rel_path("docs/.."), None);
        // 段级文件名净化：路径分隔与控制字符拒绝
        assert_eq!(sanitize_rel_path("a\\b"), None);
        assert_eq!(sanitize_rel_path("a\u{0}b/c"), None);
        // 段数上限 16
        let ok16 = (0..16).map(|i| i.to_string()).collect::<Vec<_>>().join("/");
        assert!(sanitize_rel_path(&ok16).is_some());
        let over16 = (0..17).map(|i| i.to_string()).collect::<Vec<_>>().join("/");
        assert_eq!(sanitize_rel_path(&over16), None);
    }

    #[test]
    fn sanitize_name_strips_path_components() {
        assert_eq!(sanitize_name("a.txt"), Some("a.txt".into()));
        assert_eq!(sanitize_name("/etc/passwd"), Some("passwd".into()));
        assert_eq!(sanitize_name("C:\\evil\\a.txt"), Some("a.txt".into()));
        assert_eq!(sanitize_name("../a.txt"), Some("a.txt".into()));
        assert_eq!(sanitize_name("  a b.txt  "), Some("a b.txt".into()));
        assert_eq!(sanitize_name(""), None);
        assert_eq!(sanitize_name(".."), None);
        assert_eq!(sanitize_name("a\u{0}b"), None);
    }

    /// 原子不覆盖创建：同名自动 _N 后缀，已有文件内容不被截断（create_new 语义）
    #[tokio::test]
    async fn open_unique_file_creates_without_overwrite() {
        let dir = std::env::temp_dir().join(format!("rc-upload-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // 首选原名创建
        let (p1, mut f1) = open_unique_file(&dir, "a.txt").await.unwrap();
        assert_eq!(p1, dir.join("a.txt"));
        use tokio::io::AsyncWriteExt as _;
        f1.write_all(b"original").await.unwrap();
        f1.sync_all().await.unwrap();

        // 同名 → a_1.txt；原文件内容不被截断
        let (p2, f2) = open_unique_file(&dir, "a.txt").await.unwrap();
        assert_eq!(p2, dir.join("a_1.txt"));
        drop(f2);
        assert_eq!(std::fs::read(dir.join("a.txt")).unwrap(), b"original");

        // 无扩展名 → b_1
        let (p3, f3) = open_unique_file(&dir, "b").await.unwrap();
        assert_eq!(p3, dir.join("b"));
        drop(f3);
        // 隐藏文件整体视为无扩展名：先占用原名 → 后缀
        std::fs::write(dir.join(".gitignore"), "x").unwrap();
        let (p4, f4) = open_unique_file(&dir, ".gitignore").await.unwrap();
        assert_eq!(p4, dir.join(".gitignore_1"));
        drop(f4);
        // 后缀计数中断续接：再建一个 a.txt → a_2.txt
        let (p5, f5) = open_unique_file(&dir, "a.txt").await.unwrap();
        assert_eq!(p5, dir.join("a_2.txt"));
        drop(f5);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
