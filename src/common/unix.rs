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
            let Some(process) = processes.iter().find(|p| p.ppid == shell_pid && p.tty == tty) else {
                return ClientType::Unknown;
            };
            dbg!(&process);
            if process.comm == "docker" {
                return ClientType::DockerClient(process.clone());
            }
            return ClientType::Unknown;
        }
    }
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
        let cmdline = std::fs::read(format!("/proc/{pid}/cmdline"))
            .map(|data| {
                let s = String::from_utf8_lossy(&data).replace('\0', " ");
                s.trim().to_string()
            })
            .unwrap_or_default();
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

    #[test]
    fn test_proc_stdin_tty() {
        // 当前进程 stdin 控制终端路径
        let pid = std::process::id();
        let tty1 = proc_stdin_tty(Some(pid));
        let tty2 = proc_stdin_tty(None);
        dbg!(&tty1, &tty2);
        assert_eq!(tty1, tty2);
        assert!(tty1.is_some());
        // init 进程没有 stdin 控制终端路径
        let tty = proc_stdin_tty(Some(1));
        dbg!(&tty);
        assert!(tty.is_none());
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
