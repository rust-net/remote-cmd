use portable_pty::{Child, CommandBuilder, MasterPty, PtySize};
use std::io::Read;

pub fn run(
    program: &str,
    args: &[&str],
    cwd: Option<&str>,
) -> Option<(Box<dyn MasterPty + Send>, Box<dyn Child + Send + Sync>)> {
    let pty_system = portable_pty::native_pty_system();
    let pair = match pty_system.openpty(PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(p) => p,
        Err(e) => {
            macro_log::w!("打开 PTY 失败: {:?}", e);
            return None;
        }
    };

    let mut cmd = CommandBuilder::new(program);
    cmd.args(args);
    // 嵌套终端程序（tmux attach / docker exec / vim 等）依赖 TERM 初始化，
    // 缺失会导致 tmux attach 直接失败（open terminal failed）
    cmd.env("TERM", "xterm-256color");
    if let Some(cwd) = cwd {
        cmd.cwd(cwd);
    }
    let child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(err) => {
            macro_log::w!("PTY 启动进程失败 {}: {err}", program);
            return None;
        }
    };
    drop(pair.slave); // 父进程立即释放 slave 端
    Some((pair.master, child))
}

pub fn read_pty_output(master_pty: &mut Box<dyn MasterPty + Send>) -> String {
    let mut output = String::new();
    let mut reader = master_pty.try_clone_reader().expect("克隆 PTY 读端失败");
    reader
        .read_to_string(&mut output)
        .expect("读取 PTY 输出失败");
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pid() {
        let (mut master, child) = run("sh", &["-c", "echo -n $$"], None).unwrap();
        let pid = child.process_id();
        assert!(pid.is_some());
        assert!(pid.unwrap() > 0);
        let output = read_pty_output(&mut master);
        let sh_pid = output.parse::<u32>().unwrap();
        assert_eq!(sh_pid, pid.unwrap());
    }

    #[test]
    fn test_pwd() {
        let (mut master, _child) = run("sh", &["-c", "echo -n $(pwd)"], None).unwrap();
        let output = read_pty_output(&mut master);
        let home_dir = std::env::home_dir().unwrap().display().to_string();
        assert_eq!(home_dir, output);

        let (mut master, _child) = run("sh", &["-c", "echo -n $(pwd)"], Some("/tmp")).unwrap();
        let stdout = read_pty_output(&mut master);
        assert_eq!("/tmp", stdout);
    }

    #[test]
    fn test_tty() {
        let (mut master, _child) = run("sh", &["-c", "echo -n $(tty)"], None).unwrap();
        let output = read_pty_output(&mut master);
        let tty = master.tty_name().unwrap();
        // 动态链接器警告等宿主环境噪声可能混入输出，只做包含断言
        assert!(output.contains(tty.to_str().unwrap()), "tty={tty:?} output={output:?}");
    }

    #[tokio::test]
    async fn test_kill() {
        let (mut master, mut child) = run("sh", &["-c", "ping 1.1.1.1"], None).unwrap();

        // read_to_string 会阻塞到 EOF，必须放到 blocking 线程
        let reader_task = tokio::task::spawn_blocking(move || {
            read_pty_output(&mut master)
        });

        // 进程存活期间不应有 EOF
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!reader_task.is_finished());

        // kill → SIGKILL → wait 立即返回非成功状态
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());

        // kill 后 EOF 立即到达，读取结束（timeout 兜底防死锁）
        let output= tokio::time::timeout(std::time::Duration::from_secs(5), reader_task)
            .await
            .expect("kill 后 5s 内未读到 EOF")
            .unwrap();
        assert!(output.contains("1.1.1.1"));
    }
}
