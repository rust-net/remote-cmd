#[cfg(unix)]
pub fn get_cwd(pid: u32) -> Result<String, std::io::Error> {
    let path = std::fs::read_link(format!("/proc/{}/cwd", pid))?;
    Ok(path.to_string_lossy().into_owned())
}

/// 非 Unix 平台无公开接口读取其他进程的 cwd（Windows 无 /proc），
/// 调用方按失败回退默认目录处理
#[cfg(not(unix))]
#[allow(unused)] // Windows 服务端不参与 cwd 解析链，保留接口对称性
pub fn get_cwd(_pid: u32) -> Result<String, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "非 Unix 平台不支持读取其他进程 cwd",
    ))
}

/// 上传落地目标：宿主路径或容器内路径（仅 unix 解析链使用）
#[derive(Debug)]
#[cfg(unix)]
pub enum ProcessCwd {
    /// 宿主文件系统路径
    Host(String),
    /// docker exec 会话：直接写入容器内（见 common/docker.rs）
    Container(crate::common::docker::ContainerCwd),
}

/// 上传落地目标解析（Unix）：tmux / docker exec 感知的进程 cwd 解析链。
/// 1. tmux 检测：tmux 的 pane shell 由 tmux server（守护进程，脱离我们的 PTY）启动，
///    外层注册 shell 的 /proc cwd 在用户 cd 进 tmux 后不再变化——因此先探测 tmux
///    客户端，读取活动 pane 的实时 cwd；pane 前台是 docker 客户端（tmux 内
///    `docker exec`）时改走容器内 cwd 解析（见下）；
/// 2. docker exec 检测：shell 直接子进程为 docker 客户端时，经 docker 事件跟踪的
///    exec 会话（或容器内枚举兜底）解析容器内实时 cwd，文件直接推入容器；
/// 3. 无 tmux / 非 docker exec / 解析失败 → /proc/<pid>/cwd（普通 shell 的实时 cwd）。
#[cfg(unix)]
pub async fn resolve_process_cwd(pid: u32) -> Option<ProcessCwd> {
    use crate::common::unix::*;

    match find_client_type(pid).await {
        ClientType::TmuxClient(tmux) => {
            let Some(pane) = tmux.get_active_pane().await else {
                return None;
            };
            // pane 前台为 docker 客户端（tmux 内 docker exec）→ 容器内 cwd；
            // 注意 pane_pid 是 pane 的 shell，docker 客户端是其子进程，
            // 经 find_client_type 定位子进程并取其 cmdline（含容器引用）。
            // 容器侧解析失败回退 pane_current_path（宿主视角）
            if pane.pane_current_command == "docker" {
                let docker_info = match find_client_type(pane.pane_pid).await {
                    ClientType::DockerClient(info) => Some(info),
                    _ => None,
                };
                let resolved = match docker_info {
                    Some(info) => crate::common::docker::resolve_container_cwd(&info.cmdline, info.pid).await,
                    None => None,
                };
                if let Some(cwd) = resolved {
                    macro_log::d!(
                        "[cwd] tmux 内 docker exec，容器 {} 内 cwd {}",
                        cwd.container_ref,
                        cwd.container_cwd
                    );
                    return Some(ProcessCwd::Container(cwd));
                }
                macro_log::d!(
                    "[cwd] tmux pane 前台 docker 客户端解析失败，回退 pane_current_path={}",
                    pane.pane_current_path
                );
            }
            Some(ProcessCwd::Host(pane.pane_current_path))
        }
        ClientType::DockerClient(info) => match crate::common::docker::resolve_container_cwd(&info.cmdline, info.pid).await {
            Some(cwd) => {
                macro_log::d!(
                    "[cwd] docker exec 容器 {} 内 cwd {}",
                    cwd.container_ref,
                    cwd.container_cwd
                );
                Some(ProcessCwd::Container(cwd))
            }
            // 容器 cwd 不可得（无跟踪会话且容器内枚举失败）→ 外层 shell 的宿主 cwd
            None => get_cwd(pid).ok().map(ProcessCwd::Host),
        },
        _ => get_cwd(pid).ok().map(ProcessCwd::Host),
    }
}

/// 非 Unix：无 /proc 与 tmux 语义，直接回退默认目录
#[cfg(not(unix))]
pub async fn resolve_process_cwd(_pid: u32) -> Option<String> {
    None
}
