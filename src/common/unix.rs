use std::time::{Duration, SystemTime};

use crate::common::cwd::get_cwd;
use crate::common::tmux;

#[derive(Debug, Clone)]
pub enum ClientType {
    TmuxClient(tmux::TmuxClient),
    DockerClient(ProcessInfo),
    Unknown,
}

/// 查询 Shell 进程前台运行的客户端类型
pub async fn find_client_type(shell_pid: u32) -> ClientType {
    let Some(tty) = proc_stdin_tty(Some(shell_pid)) else {
        return ClientType::Unknown;
    };
    let tmux_clients = tmux::query_tmux_clients(None).await;
    match tmux_clients
        .iter()
        .filter(|tmux| tmux.client_name == tty)
        .next()
    {
        Some(tmux) => ClientType::TmuxClient(tmux.to_owned()),
        None => {
            let processes = enumerate_processes(Some(tty.to_owned()));
            let Some(process) = processes.iter().find(|p| p.ppid == shell_pid) else {
                return ClientType::Unknown;
            };
            if process.comm == "docker" {
                return ClientType::DockerClient(process.clone());
            }
            return ClientType::Unknown;
        }
    }
}

/// 读取进程完整命令行（/proc/<pid>/cmdline，NUL 转空格；不可读/为空返回 None）
pub fn proc_cmdline(pid: u32) -> Option<String> {
    std::fs::read(format!("/proc/{pid}/cmdline"))
        .ok()
        .map(|data| String::from_utf8_lossy(&data).replace('\0', " ").trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 进程启动时刻（/proc/<pid>/stat 第 22 字段为时钟滴答数，自系统启动起算，
/// 加 /proc/stat 的 btime 得绝对时间；用于跨进程时间相关，不可读返回 None）。
/// Linux 用户态时钟频率取常规值 100Hz
pub fn proc_start_time(pid: u32) -> Option<SystemTime> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = stat.rsplit(')').next()?;
    let ticks: u64 = rest.split_whitespace().nth(19)?.parse().ok()?;
    let btime = std::fs::read_to_string("/proc/stat").ok()?;
    let btime: u64 = btime
        .lines()
        .find_map(|l| l.strip_prefix("btime "))?
        .trim()
        .parse()
        .ok()?;
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(btime + ticks / 100))
}

/// 获取进程 stdin 控制终端路径（/dev/pts/N、/dev/tty、/dev/pty）
///
/// `pid`：进程 ID，`None` 表示当前进程
pub fn proc_stdin_tty(pid: Option<u32>) -> Option<String> {
    let pid = match pid {
        None => "self",
        Some(pid) => &format!("{}", pid),
    };
    let path = std::fs::read_link(format!("/proc/{pid}/fd/0")).ok()?;
    let s = path.to_string_lossy();
    // 只接受 tty 设备路径
    if s.starts_with("/dev/pts/") || s.starts_with("/dev/tty") || s.starts_with("/dev/pty") {
        Some(s.into_owned())
    } else {
        None
    }
}

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    pub ppid: u32,
    pub tty: String,
    pub comm: String,
    pub cmdline: String,
    pub cwd: String,
}

/// 枚举进程（/proc 遍历：ppid 取自 stat 第 4 字段，tty 为 stdin 控制终端路径，
/// comm/cmdline 取自 stat 与 cmdline，cwd 为工作目录。不可读的进程跳过）。
/// `tty` 传入 `Some(path)` 时只保留该控制终端上的进程
pub fn enumerate_processes(tty: Option<String>) -> Vec<ProcessInfo> {
    let mut processes = Vec::new();
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return processes;
    };
    for entry in dir.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let proc_tty = proc_stdin_tty(Some(pid));
        // 指定了 tty 过滤时，不匹配的进程直接跳过
        if let Some(filter) = &tty {
            if proc_tty.as_deref() != Some(filter.as_str()) {
                continue;
            }
        }
        let tty = proc_tty.unwrap_or_default();
        // /proc/<pid>/stat: pid (comm) state ppid ...，comm 可能含 ')' 与空格，从最后一个 ')' 后解析
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        let Some(open) = stat.find('(') else {
            continue;
        };
        let Some(close) = stat.rfind(')') else {
            continue;
        };
        let comm = stat[open + 1..close].to_string();
        let mut fields = stat[close + 1..].split_whitespace();
        let _state = fields.next();
        let Some(ppid) = fields.next().and_then(|f| f.parse::<u32>().ok()) else {
            continue;
        };
        // 完整命令行：cmdline 以 NUL 分隔；内核线程 cmdline 为空则回退 comm
        let cmdline = proc_cmdline(pid).unwrap_or_default();
        let cwd = get_cwd(pid).unwrap_or_default();
        processes.push(ProcessInfo {
            pid,
            ppid,
            tty,
            comm,
            cmdline,
            cwd,
        });
    }
    processes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pty;

    #[tokio::test]
    async fn test_find_client_type() {
        let (master, mut child) = pty::run(
            "sh",
            &[
                "-c",
                // 启动 tmux 会话，前台运行 docker 客户端
                "tmux kill-session -t demo; tmux new -d -s demo && tmux send-keys -t demo 'cd /tmp && docker exec -it alpine sh' Enter && tmux attach -t demo",
            ],
            None,
        )
        .unwrap();

        let child_pid = child.process_id().unwrap();
        let tty_name = master.tty_name().unwrap();
        let proc_tty = proc_stdin_tty(Some(child_pid)).unwrap();
        dbg!(child_pid, &tty_name, &proc_tty);
        assert_eq!(proc_tty, tty_name);

        tokio::time::sleep(std::time::Duration::from_secs(1)).await; // 等待 tmux attach 完成
        let tmux = find_client_type(child_pid).await;
        dbg!(&tmux);
        match tmux {
            ClientType::TmuxClient(tmux) => {
                assert_eq!(&tmux.session_name, "demo");
                assert_eq!(&tmux.client_tty, &tty_name);

                let pane = tmux.get_active_pane().await.unwrap();
                dbg!(&pane);
                assert_eq!(pane.pane_current_path, "/tmp");
                assert_eq!(pane.pane_current_command, "docker");

                let docker = find_client_type(pane.pane_pid).await;
                dbg!(&docker);

                child.kill().unwrap();
                assert!(child.wait().is_ok());
                assert_ne!(child.wait().unwrap().exit_code(), 0);
            }
            _ => {
                unreachable!()
            }
        }
    }

    /// docker exec cwd 解析端到端：tmux pane 内启动 `docker exec`（pane 前台为
    /// docker 客户端），经事件跟踪的 exec 会话解析容器内实时 cwd。
    /// 依赖本机 docker daemon 与 alpine 镜像；自建测试容器
    #[tokio::test]
    async fn test_resolve_docker_exec_cwd() {
        use std::time::Duration;

        // 启动 exec 事件监听（OnceLock 全局共享，重复调用幂等），等待连接建立
        crate::common::docker::spawn_watcher();
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 自建测试容器，不依赖环境既有容器状态
        std::process::Command::new("docker")
            .args(["rm", "-f", "rc-it-exec"])
            .status()
            .expect("docker CLI 不可用");
        std::process::Command::new("docker")
            .args(["run", "-d", "--name", "rc-it-exec", "alpine", "sleep", "600"])
            .status()
            .expect("docker run 失败");

        let (master, mut child) = pty::run(
            "sh",
            &[
                "-c",
                // pane 内 cd /tmp 后 docker exec 进容器并 cd /etc
                "tmux kill-session -t demo3 2>/dev/null; tmux new -d -s demo3 && tmux send-keys -t demo3 'cd /tmp && docker exec -it rc-it-exec sh -c \"cd /etc && exec sh\"' Enter && tmux attach -t demo3",
            ],
            None,
        )
        .unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await; // 等待 attach 与 exec 会话建立

        let tty = master.tty_name().unwrap().to_string_lossy().to_string();
        let ClientType::TmuxClient(tmux) = find_client_type(child.process_id().unwrap()).await
        else {
            panic!("外层 shell 未识别为 tmux 客户端");
        };
        assert_eq!(tty, tmux.client_tty);
        let pane = tmux.get_active_pane().await.unwrap();
        assert_eq!(pane.pane_current_command, "docker");

        // pane 前台为 docker 客户端 → DockerClient 分支 → 容器 cwd 解析
        let ClientType::DockerClient(info) = find_client_type(pane.pane_pid).await else {
            panic!("pane 前台未识别为 docker 客户端");
        };
        let cwd = crate::common::docker::resolve_container_cwd(&info.cmdline, info.pid)
            .await
            .expect("容器 cwd 解析失败");
        assert_eq!(cwd.container_cwd, "/etc");
        // 容器引用与 ID 均已解析（文件将直接推入容器）
        assert_eq!(cwd.container_ref, "rc-it-exec");
        assert!(cwd.container_id.len() >= 12);

        // 全链路：外层注册 shell → TmuxClient → pane 前台 docker 客户端判定 → 容器 cwd
        // （tmux 内 docker exec 场景，不再回退宿主 pane_current_path）
        match crate::common::cwd::resolve_process_cwd(child.process_id().unwrap()).await {
            Some(crate::common::cwd::ProcessCwd::Container(cwd)) => {
                assert_eq!(cwd.container_cwd, "/etc");
            }
            other => panic!("期望容器内 cwd，实际 {other:?}"),
        }

        child.kill().unwrap();
        child.wait().unwrap();
        std::process::Command::new("docker")
            .args(["rm", "-f", "rc-it-exec"])
            .status()
            .expect("docker rm 失败");
    }

    #[test]
    fn test_proc_stdin_tty() {
        // 测试进程自身的 stdin 未必是终端（cargo test 下常为管道），用 PTY 造真实控制终端
        let (master, mut child) = pty::run("sleep", &["2"], None).unwrap();
        let pid = child.process_id().unwrap();
        let tty = proc_stdin_tty(Some(pid)).unwrap();
        assert_eq!(master.tty_name().unwrap().to_string_lossy(), tty);
        // init 进程没有 stdin 控制终端路径
        let tty = proc_stdin_tty(Some(1));
        dbg!(&tty);
        assert!(tty.is_none());
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn test_enumerate_processes() {
        let processes = enumerate_processes(None);
        dbg!(&processes);
        assert!(!processes.is_empty());
        // init 进程存在且 ppid 为 0，无控制终端
        let init = processes.iter().find(|p| p.pid == 1).unwrap();
        assert_eq!(init.ppid, 0);
        assert_eq!(init.tty, "");
        assert_eq!(init.comm, "systemd");
        assert!(!init.cmdline.is_empty());
        // 当前测试进程存在，cmd/cwd 已填充，且 ppid 指向的父进程也在枚举结果中
        let me = std::process::id();
        let mine = processes.iter().find(|p| p.pid == me).unwrap();
        assert!(mine.cmdline.contains("remote_cmd") || mine.cmdline.contains("remote-cmd"));
        assert!(!mine.cwd.is_empty());
        assert!(processes.iter().any(|p| p.pid == mine.ppid));
    }

    #[test]
    fn test_enumerate_processes_tty_filter() {
        // 用 PTY 造一个真实控制终端（不依赖测试环境自身的 stdin 是否为 tty）
        let (master, mut child) = pty::run("sleep", &["10"], None).unwrap();
        let tty = master.tty_name().unwrap().to_string_lossy().to_string();

        let processes = enumerate_processes(Some(tty.clone()));
        dbg!(&processes);
        let pid = child.process_id().unwrap();
        let mine = processes.iter().find(|p| p.pid == pid).unwrap();
        assert_eq!(mine.tty, tty);
        assert_eq!(mine.comm, "sleep");

        // 过滤不存在的 tty → 空
        assert!(enumerate_processes(Some("/dev/pts/9999".into())).is_empty());

        child.kill().unwrap();
        child.wait().unwrap();
    }
}
