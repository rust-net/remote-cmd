//! docker exec 场景的容器 cwd 解析。
//!
//! 背景：用户在外层 shell 中 `docker exec -it <ctr> sh` 后，真实工作目录在容器
//! 命名空间内，宿主侧 /proc 无法直接观测（docker 客户端进程的 cwd 是启动 exec
//! 时的宿主目录，不是容器内实时的 cwd）。
//!
//! 方案：后台常驻任务监听 docker daemon 事件流（exec_create / exec_start /
//! exec_die），维护「容器 → 存活 exec 会话」表。查询时：
//! 1. 从 docker 客户端进程 cmdline 提取容器引用（名称或 ID）；
//! 2. inspect_container 定位容器，取该容器上最近创建且存活的 exec 会话；
//! 3. inspect_exec 拿到 exec 进程 pid（宿主命名空间），经 NSpid 映射为容器内 pid；
//! 4. 在容器内新开 exec 执行 `readlink /proc/<pid>/cwd` 得容器内 cwd；
//! 5. 按容器 bind mount / volume 映射回宿主路径（无映射返回 None，调用方回退）。
//!
//! 兜底：事件流可能丢投递（watcher 启动前 / daemon 重启 / 环境异常），无跟踪会话
//! 时改为在容器内枚举分配了 tty 的 shell（最近一次 docker exec 会话）直接读 cwd。
//!
//! 平台：仅 Unix 编译（bollard 依赖只挂 unix target）；调用点（state.rs / upload.rs）
//! 按 `#[cfg(unix)]` 门控，非 Unix 无容器上传能力。

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use bollard::models::EventMessage;
use bollard::query_parameters::{EventsOptionsBuilder, UploadToContainerOptionsBuilder};
use bollard::Docker;
use futures::StreamExt;

/// 全局 exec 会话表：由 spawn_watcher 在服务启动时填充，docker 不可用时为空
static TRACKER: OnceLock<ExecTracker> = OnceLock::new();

/// 服务端自己发起的辅助 exec（同名探测/容器内枚举/mkdir 等）的 id 集。
/// 事件流里遇到这些 id 直接跳过，避免污染用户的 exec 会话登记表：
/// 我们无法在事件属性上区分 `-it` 与否，但辅助 exec 全部经 create_own_exec
/// 创建，登记 id 即可精确识别
static OWN_EXECS: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();

fn own_execs() -> &'static Mutex<std::collections::HashSet<String>> {
    OWN_EXECS.get_or_init(|| Mutex::new(std::collections::HashSet::new()))
}

fn mark_own_exec(exec_id: &str) {
    own_execs().lock().unwrap_or_else(|e| e.into_inner()).insert(exec_id.to_string());
}

fn take_own_exec(exec_id: &str) -> bool {
    own_execs().lock().unwrap_or_else(|e| e.into_inner()).remove(exec_id)
}

/// 创建服务端自身的辅助 exec 并登记 id（exec_die 后由事件处理清除）
async fn create_own_exec(
    docker: &Docker,
    container_id: &str,
    cmd: Vec<String>,
) -> anyhow::Result<bollard::exec::CreateExecResults> {
    let created = docker
        .create_exec(
            container_id,
            CreateExecOptions {
                cmd: Some(cmd),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..Default::default()
            },
        )
        .await
        .context("exec 创建失败")?;
    mark_own_exec(&created.id);
    Ok(created)
}

/// 容器内 cwd 解析结果（上传直接写入容器内，不做宿主 bind 映射）
#[derive(Clone, Debug)]
pub struct ContainerCwd {
    /// 容器完整 ID（后续 archive/exec API 调用使用）
    pub container_id: String,
    /// docker 客户端 cmdline 中的容器引用（名称或短 ID，展示用）
    pub container_ref: String,
    /// 容器命名空间内的当前工作目录
    pub container_cwd: String,
}

#[derive(Default)]
struct ExecTracker {
    /// exec_id → 会话信息（仅含已 exec_start 的会话）
    sessions: Mutex<HashMap<String, ExecSession>>,
}

struct ExecSession {
    /// 容器标识（事件 attributes 只带容器名；个别版本为完整 ID）
    container: String,
    /// 会话登记时刻（事件时间）：用于与 docker 客户端进程启动时间相关，
    /// 区分同容器的多个 exec 会话
    started_at: SystemTime,
}

/// exec 会话过期时间：正常交互会话远短于此；兜底防泄漏
const SESSION_TTL: Duration = Duration::from_secs(24 * 3600);

impl ExecTracker {
    fn insert(&self, exec_id: String, container: String, started_at: SystemTime) {
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let now = SystemTime::now();
        // TTL 兜底清理；事件时间略超前于本机时钟（时钟偏差）时保留，不能误删
        sessions.retain(|_, s| {
            now.duration_since(s.started_at).map(|d| d < SESSION_TTL).unwrap_or(true)
        });
        sessions.insert(exec_id, ExecSession { container, started_at });
    }

    fn remove(&self, exec_id: &str) {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner()).remove(exec_id);
    }

    /// 指定容器上与本客户端关联的 exec 会话 id（按容器名或完整 ID 任一匹配：
    /// 事件 attributes 只带容器名，查询侧两者都有）。
    /// 同容器多会话时按 docker 客户端进程启动时间相关：exec_start 事件紧随
    /// 客户端启动（1s 容差），登记时间与之最接近者即本客户端的会话；
    /// 客户端启动时间未知时回退取最近登记的会话。
    fn latest_of(&self, containers: &[&str], client_start: Option<SystemTime>) -> Option<String> {
        let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let candidates: Vec<(&String, &ExecSession)> = sessions
            .iter()
            .filter(|(_, s)| containers.iter().any(|c| *c == s.container))
            .filter(|(_, s)| match client_start {
                // 事件不可能早于客户端进程启动（容差吸收时钟粒度）
                Some(t) => s.started_at + Duration::from_secs(1) >= t,
                None => true,
            })
            .collect();
        match client_start {
            Some(t) => candidates
                .into_iter()
                .min_by_key(|(_, s)| s.started_at.duration_since(t).unwrap_or(Duration::ZERO))
                .map(|(id, _)| id.clone()),
            None => candidates
                .into_iter()
                .max_by_key(|(_, s)| s.started_at)
                .map(|(id, _)| id.clone()),
        }
    }
}

/// 启动 exec 事件监听后台任务：docker daemon 不可达时退避重试（daemon 可能晚于
/// 本服务启动，如 systemd 并行拉起；上限 5 分钟避免无 docker 环境的空转）。
/// 连接成功后事件流中断（daemon 重启等）自动重连。
pub fn spawn_watcher() {
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(5);
        loop {
            let Some(docker) = connect().await else {
                macro_log::d!("[docker] docker daemon 不可达，{}s 后重试", backoff.as_secs());
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(300));
                continue;
            };
            let _ = TRACKER.set(ExecTracker::default());
            macro_log::i!("[docker] 已连接 docker daemon，开始跟踪 exec 会话");
            loop {
                let err = run_event_loop(&docker).await;
                macro_log::w!("[docker] 事件流中断: {err}，5s 后重连");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });
}

async fn connect() -> Option<Docker> {
    let docker = Docker::connect_with_local_defaults().ok()?;
    // 版本协商：bollard 默认请求 1.53，高于 daemon 最高支持版本时
    // （/version 端点免校验可通，但 exec/archive 等会被拒），必须降级到 daemon 版本
    let docker = docker.negotiate_version().await.ok()?;
    Some(docker)
}

async fn run_event_loop(docker: &Docker) -> String {
    let since = chrono::Utc::now().timestamp().to_string();
    let mut filters = HashMap::new();
    // 注意：exec 事件（exec_create/exec_start/exec_die）的 Type 为 "container"，
    // Docker API 并无 type=exec 过滤值；仅过滤容器事件，exec_* 动作在客户端侧判定
    filters.insert("type", vec!["container"]);
    filters.insert("event", vec!["exec_start", "exec_die"]);
    let options = EventsOptionsBuilder::new().since(&since).filters(&filters).build();
    let mut events = docker.events(Some(options)).fuse();
    while let Some(msg) = events.next().await {
        let Ok(msg) = msg else {
            return "event stream error".to_string();
        };
        handle_event(msg);
    }
    "event stream closed".to_string()
}

/// 处理单条事件：exec_start 登记会话、exec_die 移除；其余打点便于诊断事件流投递。
/// exec_create 不登记（未启动的 exec 不构成"用户正在其中"的会话）。
/// 注意：exec 事件的 attributes 无 `container` 字段，容器标识在 `name`（容器名）；
/// `container` 字段仅在部分 daemon 版本出现，两者任取其一。
fn handle_event(msg: EventMessage) {
    let Some(tracker) = TRACKER.get() else { return };
    let Some(actor) = msg.actor else { return };
    let attrs = actor.attributes.unwrap_or_default();
    // exec 事件的 Actor.ID 即 exec id；部分版本经 execID 属性冗余携带，优先取属性
    let exec_id = attrs
        .get("execID")
        .cloned()
        .or(actor.id.clone())
        .unwrap_or_default();
    let container = attrs
        .get("container")
        .or_else(|| attrs.get("name"))
        .cloned()
        .unwrap_or_default();
    let action = msg.action.clone().unwrap_or_default();
    // 服务端自身的辅助 exec（探测/枚举/mkdir）不进登记表；exec_die 后清除登记
    if own_execs().lock().unwrap_or_else(|e| e.into_inner()).contains(&exec_id) {
        if action.starts_with("exec_die") {
            take_own_exec(&exec_id);
        }
        // macro_log::d!("[docker] 跳过自身辅助 exec 事件 action={action} exec={exec_id}");
        return;
    }
    macro_log::d!("[docker] 事件 typ={:?} action={action} exec={exec_id} container={container}", msg.typ);
    if exec_id.is_empty() || container.is_empty() {
        return;
    }
    if action.starts_with("exec_start") {
        let started_at = msg
            .time
            .and_then(|t| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(t as u64)))
            .unwrap_or_else(SystemTime::now);
        tracker.insert(exec_id, container, started_at);
    } else if action.starts_with("exec_die") {
        tracker.remove(&exec_id);
    }
}

/// docker exec 场景的 cwd 解析入口（cwd.rs resolve_process_cwd 调用）。
/// `cmdline` 为 docker 客户端进程完整命令行（从中提取容器引用）；`client_pid`
/// 为该客户端进程 pid（用于多会话同容器时按启动时间关联 exec 会话）。
/// 优先用事件跟踪的 exec 会话精确定位；无跟踪会话（事件流中断 / watcher 刚启动 /
/// daemon 重启）时回退容器内枚举发现。整体失败返回 None，调用方回退宿主 shell cwd。
pub async fn resolve_container_cwd(cmdline: &str, client_pid: u32) -> Option<ContainerCwd> {
    let Some(container_ref) = container_ref_from_cmdline(cmdline) else {
        macro_log::d!("[cwd] docker 客户端 cmdline 未识别容器引用: {cmdline:?}");
        return None;
    };
    macro_log::d!("[cwd] docker exec 容器引用: {container_ref}");

    let docker = connect().await?;
    let desc = match docker.inspect_container(&container_ref, None).await {
        Ok(desc) => desc,
        Err(err) => {
            macro_log::d!("[cwd] 容器 {container_ref} inspect 失败: {err}");
            return None;
        }
    };
    let container_id = desc.id.clone()?;

    let container_cwd = match tracked_container_cwd(&docker, &container_ref, &container_id, client_pid).await {
        Some(cwd) => cwd,
        None => {
            // 常见于会话早于监听器启动（exec_start 事件在 since 之前，无法补登记）
            macro_log::d!("[cwd] 跟踪表未命中（会话可能早于监听器启动），容器内枚举发现");
            discover_container_cwd(&docker, &container_id, client_pid).await?
        }
    };
    macro_log::d!("[cwd] 容器内 cwd: {container_cwd}");
    Some(ContainerCwd { container_id, container_ref, container_cwd })
}

/// 经事件跟踪的 exec 会话解析容器 cwd：按客户端启动时间关联会话 → inspect_exec
/// 校验存活 → 宿主 pid 映射容器 pid → 容器内 readlink。任一步失败返回 None（调用方兜底）。
async fn tracked_container_cwd(
    docker: &Docker,
    container_ref: &str,
    container_id: &str,
    client_pid: u32,
) -> Option<String> {
    let tracker = TRACKER.get()?;
    let client_start = crate::common::unix::proc_start_time(client_pid);
    let exec_id = tracker.latest_of(&[container_ref, container_id], client_start)?;
    let Ok(inspect) = docker.inspect_exec(&exec_id).await else {
        macro_log::d!("[cwd] exec {exec_id} inspect 失败（会话可能已结束）");
        return None;
    };
    if !inspect.running.unwrap_or(false) {
        macro_log::d!("[cwd] exec {exec_id} 已退出");
        tracker.remove(&exec_id);
        return None;
    }
    let Some(host_pid) = inspect.pid.map(|p| p as u32) else {
        macro_log::d!("[cwd] exec {exec_id} 无 pid");
        return None;
    };
    // inspect_exec 的 pid 是宿主命名空间 pid；NSpid 末位为容器命名空间 pid。
    // 容器与宿主共享 pid ns（如 host pid ns 容器）时两者相同，无需映射。
    let container_pid = host_nspid_last(host_pid).unwrap_or(host_pid);
    macro_log::d!("[cwd] exec {exec_id} pid: host={host_pid} container={container_pid}");
    container_cwd(docker, container_id, container_pid).await
}

/// 无跟踪会话时的兜底：在容器内枚举"分配了 tty 的交互 shell"（`docker exec -it`
/// 会为 shell 分配 /dev/pts/N；宿主 dockerd 无法感知其容器内 pid，事件流也可能
/// 丢投递）。多会话同容器时与登记表同样按客户端启动时间相关：容器内
/// /proc/<pid>/stat 的 starttime 滴答与宿主同一时钟，换算后取"启动时刻紧随
/// 本客户端"的 shell。探测 exec 未分配 tty（fd/0 为管道/套接字），不会命中自身。
async fn discover_container_cwd(docker: &Docker, container_id: &str, client_pid: u32) -> Option<String> {
    // 输出 "pid starttime_ticks cwd"：starttime 取 stat 第 22 字段（去掉 "pid (comm)" 前缀后第 20 列）
    let script = "for p in /proc/[0-9]*; do \
        t=$(readlink $p/fd/0 2>/dev/null); \
        case $t in /dev/pts/*|/dev/pty*) \
            c=$(readlink $p/cwd 2>/dev/null) || continue; \
            st=$(sed 's/.*) //' $p/stat 2>/dev/null | cut -d' ' -f20); \
            echo \"${p#/proc/} ${st:-0} $c\";; esac; done";
    let out = match tokio::time::timeout(
        Duration::from_secs(5),
        exec_capture(
            docker,
            container_id,
            vec!["sh".into(), "-c".into(), script.into()],
        ),
    )
    .await
    {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            macro_log::d!("[cwd] 容器内枚举 exec 失败: {e}");
            return None;
        }
        Err(_) => {
            macro_log::d!("[cwd] 容器内枚举 exec 超时");
            return None;
        }
    };
    // 行格式 "pid ticks cwd"（cwd 可能含空格，以首个空格二次切分）
    let client_start = crate::common::unix::proc_start_time(client_pid);
    let btime = boot_time()?;
    let shells: Vec<(u32, SystemTime, String)> = out
        .lines()
        .filter_map(|l| {
            let (pid, rest) = l.split_once(' ')?;
            let pid = pid.parse::<u32>().ok()?;
            let (ticks, cwd) = rest.split_once(' ')?;
            let ticks: u64 = ticks.parse().ok()?;
            let start = SystemTime::UNIX_EPOCH + Duration::from_secs(btime + ticks / 100);
            cwd.starts_with('/').then_some((pid, start, cwd.to_string()))
        })
        .collect();
    let picked = pick_shell(&shells, client_start);
    let Some((pid, cwd)) = picked else {
        macro_log::d!("[cwd] 容器内未发现分配 tty 的 exec shell");
        return None;
    };
    macro_log::d!("[cwd] 容器内发现 exec shell pid={pid} cwd={cwd}");
    Some(cwd)
}

/// 容器内 tty shell 候选选择：优先"启动时刻紧随客户端进程启动"（与登记表同一
/// 相关策略，解决同容器多会话歧义）；客户端启动时间不可得时回退 pid 最大者
/// （最近一次 exec）。纯函数便于单元测试
fn pick_shell(
    shells: &[(u32, SystemTime, String)],
    client_start: Option<SystemTime>,
) -> Option<(u32, String)> {
    match client_start {
        Some(t) => shells
            .iter()
            .filter(|(_, start, _)| *start + Duration::from_secs(1) >= t)
            .min_by_key(|(_, start, _)| start.duration_since(t).unwrap_or(Duration::ZERO))
            .map(|(pid, _, cwd)| (*pid, cwd.clone())),
        None => shells
            .iter()
            .max_by_key(|(pid, _, _)| *pid)
            .map(|(pid, _, cwd)| (*pid, cwd.clone())),
    }
}

/// 系统启动时刻（/proc/stat 的 btime；容器内外同一时钟）
fn boot_time() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    stat.lines()
        .find_map(|l| l.strip_prefix("btime "))?
        .trim()
        .parse()
        .ok()
}

/// 从 docker 客户端 cmdline 提取容器引用：`docker exec [-flag [value]...] <ctr> <cmd>`。
/// 跳过布尔 flag 与"独立取值" flag 的值；首个裸词即容器引用。
fn container_ref_from_cmdline(cmdline: &str) -> Option<String> {
    /// 需要独立取值的 flag（值在下一个 token；内联形式 -u=root / --user=root 不受影响）
    const VALUE_FLAGS: [&str; 10] = [
        "-u", "--user", "-w", "--workdir", "-e", "--env", "--env-file", "-h", "--hostname", "-l",
    ];
    let mut after_exec = false;
    let mut skip_value = false;
    for tok in cmdline.split_whitespace() {
        if !after_exec {
            after_exec = tok == "exec";
            continue;
        }
        if skip_value {
            skip_value = false;
            continue;
        }
        if tok.starts_with('-') {
            if tok.contains('=') {
                continue; // 内联取值，无需跳过下一个
            }
            skip_value = VALUE_FLAGS.contains(&tok);
            continue;
        }
        return Some(tok.to_string());
    }
    None
}

/// 读取 /proc/<pid>/status 的 NSpid 末位（最内层命名空间 pid）；
/// 无 NSpid 行（与宿主同 ns）或不可读时返回 None
fn host_nspid_last(pid: u32) -> Option<u32> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|l| l.strip_prefix("NSpid:"))?.split_whitespace().last()?.parse().ok()
}

/// 容器内读取目标进程 cwd：宿主 dockerd 只能感知 exec 进程的宿主 pid，
/// 容器内 /proc 即容器命名空间视图，直接 readlink 目标 pid 的 cwd。
/// readlink 二进制缺失（极简镜像）时回退 sh -c；容器内进程属主不同可能
/// Permission denied，同样落到 sh（容器 exec 默认用户与 shell 一致时可用）。
async fn container_cwd(docker: &Docker, container_id: &str, pid: u32) -> Option<String> {
    let target = format!("/proc/{pid}/cwd");
    let attempts: [Vec<String>; 2] = [
        vec!["readlink".into(), "-f".into(), target.clone()],
        vec!["sh".into(), "-c".into(), format!("readlink {target}")],
    ];
    for cmd in attempts {
        let out = match tokio::time::timeout(Duration::from_secs(5), exec_capture(docker, container_id, cmd)).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                macro_log::d!("[docker] 容器内 readlink exec 失败: {e}");
                continue;
            }
            Err(_) => {
                macro_log::d!("[docker] 容器内 readlink exec 超时");
                continue;
            }
        };
        let out = out.trim();
        if out.starts_with('/') {
            return Some(out.to_string());
        }
        macro_log::d!("[docker] 容器内 readlink 输出非路径: {out:?}");
    }
    None
}

/// 容器内一次性 exec 并收集输出（调用方可限时；流 EOF 即进程退出）
async fn exec_capture(docker: &Docker, container_id: &str, cmd: Vec<String>) -> anyhow::Result<String> {
    let create = create_own_exec(docker, container_id, cmd).await?;
    let started = docker
        .start_exec(
            &create.id,
            Some(StartExecOptions { detach: false, ..Default::default() }),
        )
        .await?;
    let StartExecResults::Attached { mut output, .. } = started else {
        anyhow::bail!("exec 未附加输出流");
    };
    let mut buf = String::new();
    while let Some(Ok(chunk)) = output.next().await {
        buf.push_str(&String::from_utf8_lossy(chunk.into_bytes().as_ref()));
    }
    Ok(buf)
}

/// 容器内路径拼接（POSIX；base 以 `/` 结尾或为 `/` 时正确处理）
pub fn container_join(base: &str, rel: &str) -> String {
    if rel.is_empty() {
        return base.to_string();
    }
    format!("{}/{}", base.trim_end_matches('/'), rel)
}

/// 容器内递归建目录（mkdir -p）：收集输出并校验 exec 退出码，容器内失败
/// （权限不足 / 只读 fs / 极简镜像缺 mkdir）显式报错，调用方拒收
pub async fn mkdir_p(container_id: &str, path: &str) -> anyhow::Result<()> {
    let docker = connect().await.context("docker daemon 不可达")?;
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let created = create_own_exec(
                &docker,
                container_id,
                vec!["mkdir".to_string(), "-p".to_string(), path.to_string()],
            )
            .await
            .context("mkdir exec 创建失败")?;
            let started = docker
                .start_exec(
                    &created.id,
                    Some(StartExecOptions { detach: false, ..Default::default() }),
                )
                .await
                .context("mkdir exec 启动失败")?;
            let StartExecResults::Attached { mut output, .. } = started else {
                anyhow::bail!("mkdir exec 未附加输出流");
            };
            let mut out = String::new();
            while let Some(Ok(chunk)) = output.next().await {
                out.push_str(&String::from_utf8_lossy(chunk.into_bytes().as_ref()));
            }
            // exec 退出码校验：start_exec 对进程非零退出不报错，必须显式检查
            let inspect = docker
                .inspect_exec(&created.id)
                .await
                .context("mkdir exec 状态查询失败")?;
            if inspect.exit_code != Some(0) {
                anyhow::bail!("容器内 mkdir 失败 (exit={:?}): {}", inspect.exit_code, out.trim());
            }
            Ok(())
        },
    )
    .await
    .context("mkdir exec 超时")?
}

/// 容器内路径存在性探测（`test -e/-d` 经 sh 内建避免路径注入；容器缺 sh 时报错）
pub async fn path_exists(container_id: &str, path: &str, dir: bool) -> anyhow::Result<bool> {
    let script = if dir { "[ -d \"$1\" ]" } else { "[ -e \"$1\" ]" };
    let docker = connect().await.context("docker daemon 不可达")?;
    let out = tokio::time::timeout(
        Duration::from_secs(10),
        exec_capture(
            &docker,
            container_id,
            vec![
                "sh".into(),
                "-c".into(),
                format!("{script} && echo y || echo n"),
                "sh".into(),
                path.into(),
            ],
        ),
    )
    .await
    .context("存在性探测超时")?
    .context("存在性探测 exec 失败")?;
    Ok(out.trim().ends_with('y'))
}

/// 将宿主文件推入容器 dest_dir/dest_name：单条目 tar 经 archive API 解包
/// （daemon 原生接口，不依赖容器内有 shell；daemon 语义为同名覆盖）。
/// tar 整体驻留内存，超大文件有内存峰值（当前单文件上限 2GB）。
pub async fn push_file(
    container_id: &str,
    dest_dir: &str,
    dest_name: &str,
    local: &std::path::Path,
) -> anyhow::Result<()> {
    let mut tar_buf = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_buf);
        builder
            .append_file(dest_name, &mut std::fs::File::open(local)?)
            .context("tar 打包失败")?;
        builder.finish().context("tar 收尾失败")?;
    }
    let docker = connect().await.context("docker daemon 不可达")?;
    let options = UploadToContainerOptionsBuilder::new().path(dest_dir).build();
    tokio::time::timeout(
        Duration::from_secs(300),
        docker.upload_to_container(
            container_id,
            Some(options),
            bollard::body_stream(futures::stream::once(async move { bytes::Bytes::from(tar_buf) })),
        ),
    )
    .await
    .context("容器推送超时")?
    .context("容器 archive 推送失败")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 容器写入链路端到端（不依赖事件流）：mkdir_p → push_file → 容器内校验内容
    /// → 清理。自建测试容器，不依赖环境既有容器状态
    // 依赖 docker daemon 与 alpine 镜像，默认跳过：cargo test -- --ignored
    #[tokio::test]
    #[ignore = "依赖 docker daemon 与 alpine 镜像"]
    async fn test_container_push_file() {
        let docker = connect().await.expect("docker daemon 不可达");
        // 自建测试容器（alpine 镜像本地已有）
        std::process::Command::new("docker")
            .args(["rm", "-f", "rc-it-push"])
            .status()
            .expect("docker CLI 不可用");
        std::process::Command::new("docker")
            .args(["run", "-d", "--name", "rc-it-push", "alpine", "sleep", "600"])
            .status()
            .expect("docker run 失败");
        let desc = docker
            .inspect_container("rc-it-push", None)
            .await
            .expect("测试容器不存在");
        let id = desc.id.expect("无容器 id");

        let base = "/tmp/rc-upload-test";
        mkdir_p(&id, &format!("{base}/sub")).await.expect("mkdir_p 失败");
        let existed = path_exists(&id, &format!("{base}/sub"), true).await.expect("path_exists 失败");
        assert!(existed, "容器内目录未创建");

        let local = std::env::temp_dir().join(format!("rc-push-{}.txt", std::process::id()));
        std::fs::write(&local, b"hello-container").unwrap();
        push_file(&id, &format!("{base}/sub"), "hello.txt", &local)
            .await
            .expect("push_file 失败");

        // 容器内读回校验
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            exec_capture(&docker, &id, vec!["cat".into(), format!("{base}/sub/hello.txt")]),
        )
        .await
        .expect("cat exec 超时")
        .expect("cat exec 失败");
        assert_eq!(out.trim(), "hello-container");

        // 清理容器内测试目录与本地暂存
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            exec_capture(&docker, &id, vec!["rm".into(), "-rf".into(), base.into()]),
        )
        .await;
        let _ = std::fs::remove_file(&local);

        // 清理后不存在
        let existed = path_exists(&id, base, true).await.expect("path_exists 失败");
        assert!(!existed, "容器内清理失败");

        // 删除测试容器
        std::process::Command::new("docker")
            .args(["rm", "-f", "rc-it-push"])
            .status()
            .expect("docker rm 失败");
    }

    /// 同容器多 exec 会话按客户端启动时间相关：各客户端命中自己的会话，
    /// 无客户端时间时回退最近登记
    #[test]
    fn tracker_latest_of_correlates_client_start() {
        let tracker = ExecTracker::default();
        // 以当前时刻为基准构造会话时间（insert 内有 TTL 清理，不能用远古时间）
        let base = SystemTime::now() - Duration::from_secs(120);
        let s1 = base + Duration::from_secs(10); // 客户端 A 的会话登记时刻
        let s2 = base + Duration::from_secs(50); // 客户端 B 的会话登记时刻
        tracker.insert("exec-a".into(), "ctr".into(), s1);
        tracker.insert("exec-b".into(), "ctr".into(), s2);

        // 客户端 A 启动于 9s（exec_start 10s 紧随其后）
        let a_start = base + Duration::from_secs(9);
        assert_eq!(tracker.latest_of(&["ctr"], Some(a_start)).as_deref(), Some("exec-a"));
        // 客户端 B 启动于 49s：A 的会话被"晚于客户端启动"过滤 + 时间差更大而排除
        let b_start = base + Duration::from_secs(49);
        assert_eq!(tracker.latest_of(&["ctr"], Some(b_start)).as_deref(), Some("exec-b"));
        // 容器名或 ID 任一匹配；不匹配的容器无会话
        assert_eq!(tracker.latest_of(&["other"], Some(b_start)), None);
        // 无客户端启动时间 → 最近登记的会话
        assert_eq!(tracker.latest_of(&["ctr"], None).as_deref(), Some("exec-b"));
    }

    /// proc_start_time：存活进程返回早于当前的绝对时间；不存在进程返回 None
    #[test]
    fn proc_start_time_cases() {
        let t = crate::common::unix::proc_start_time(std::process::id()).expect("自身启动时刻应可读");
        assert!(t <= SystemTime::now());
        assert_eq!(crate::common::unix::proc_start_time(u32::MAX - 1), None);
    }

    /// 容器内 tty shell 候选选择：同容器多会话按客户端启动时间相关；
    /// 客户端时间不可得时回退 pid 最大者
    #[test]
    fn pick_shell_correlates_client_start() {
        let base = SystemTime::now() - Duration::from_secs(120);
        let s1 = base + Duration::from_secs(10); // 客户端 A 的 shell
        let s2 = base + Duration::from_secs(50); // 客户端 B 的 shell
        let shells = vec![
            (1u32, s1, "/a".to_string()),
            (2u32, s2, "/b".to_string()),
        ];
        // 客户端 A 启动于 9s → 命中 /a；客户端 B 启动于 49s → 命中 /b
        assert_eq!(
            pick_shell(&shells, Some(base + Duration::from_secs(9))),
            Some((1, "/a".to_string()))
        );
        assert_eq!(
            pick_shell(&shells, Some(base + Duration::from_secs(49))),
            Some((2, "/b".to_string()))
        );
        // 早于所有 shell 启动的客户端（异常情况）→ 容差取最近的
        assert_eq!(
            pick_shell(&shells, Some(base)),
            Some((1, "/a".to_string()))
        );
        // 无客户端时间 → pid 最大（最近会话）
        assert_eq!(pick_shell(&shells, None), Some((2, "/b".to_string())));
        // 无候选
        assert_eq!(pick_shell(&[], None), None);
    }

    /// 容器引用提取：布尔 flag、内联取值、独立取值、镜像命令词均可跳过
    #[test]
    fn container_ref_from_cmdline_cases() {
        assert_eq!(container_ref_from_cmdline("docker exec -it alpine sh").as_deref(), Some("alpine"));
        assert_eq!(container_ref_from_cmdline("docker exec -u root -w /app my-ctr bash").as_deref(), Some("my-ctr"));
        assert_eq!(container_ref_from_cmdline("docker exec --user=root alpine sh").as_deref(), Some("alpine"));
        // 独立取值 flag 的值不会被误认为容器引用
        assert_eq!(container_ref_from_cmdline("docker exec -u root busybox id").as_deref(), Some("busybox"));
        // 容器 ID 形式（64 位 hex）与 docker run 之外的路径无关
        assert_eq!(
            container_ref_from_cmdline("docker exec -i -t b361d0e3a1f5 sh -c 'echo hi'").as_deref(),
            Some("b361d0e3a1f5")
        );
        // 非 exec / 空引用
        assert_eq!(container_ref_from_cmdline("docker ps"), None);
        assert_eq!(container_ref_from_cmdline("docker exec -it"), None);
        assert_eq!(container_ref_from_cmdline("docker exec"), None);
    }

    /// NSpid 末位映射：本进程与宿主同 ns，NSpid 单值等于 pid 本身
    #[test]
    fn host_nspid_last_self() {
        assert_eq!(host_nspid_last(std::process::id()), Some(std::process::id()));
        // 不存在的进程
        assert_eq!(host_nspid_last(u32::MAX - 1), None);
    }
}
