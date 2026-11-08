use std::sync::LazyLock;

/// Pane 相关参数
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct TmuxPane {
    /// pane_id：窗格的唯一 ID，环境变量 $TMUX_PANE 也含有该值，作为查询目标时应使用 %pane_id 格式。
    pub pane_id: u32,
    /// pane_pid：窗格中第一个进程的 PID。
    pub pane_pid: u32,
    /// pane_current_command：窗格当前正在运行的命令（如 bash、vim）。
    pub pane_current_command: String,
    /// pane_current_path：窗格当前工作目录。
    pub pane_current_path: String,
}

#[allow(unused)]
impl TmuxPane {
    /// 获取窗格的查询目标字符串（%pane_id）
    pub fn target(&self) -> String {
        format!("%{}", self.pane_id)
    }
}

/// Client 相关参数
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct TmuxClient {
    /// client_tty：客户端伪终端路径（如 /dev/pts/1）。
    pub client_tty: String,
    /// client_pid：客户端进程的 PID。
    pub client_pid: u32,
    /// client_name：客户端名称。
    pub client_name: String,
    /// client_session：客户端当前附加到的会话名称。
    pub client_session: String,
    /// session_id：会话 ID，作为查询目标时应使用 $session_id 格式。
    pub session_id: u32,
    /// session_name：会话名称。
    pub session_name: String,
    /// window_id：窗口 ID，作为查询目标时应使用 @window_id 格式。
    pub window_id: u32,
    /// window_name：窗口名称。
    pub window_name: String,
}

#[allow(unused)]
impl TmuxClient {
    /// 获取客户端连接会话的查询目标字符串（$session_id）
    pub fn session_target(&self) -> String {
        format!("${}", self.session_id)
    }

    /// 获取客户端活动窗口的查询目标字符串（@window_id）
    pub fn window_target(&self) -> String {
        format!("@{}", self.window_id)
    }
}

/// Pane 查询格式化字符串
#[derive(Debug, Clone)]
struct PaneFormat;

impl PaneFormat {
    /// 分隔符
    const SEPARATOR: &str = ":";
    /// 格式化字符串
    const FORMAT: LazyLock<String> = LazyLock::new(|| {
        format!(
            r#"{1}{0}{2}{0}{3}{0}{4}"#,
            Self::SEPARATOR,
            "#{pane_id}",
            "#{pane_pid}",
            "#{pane_current_command}",
            "#{pane_current_path}"
        )
    });
}

/// Client 查询格式化字符串
#[derive(Debug, Clone)]
struct ClientFormat;

impl ClientFormat {
    /// 分隔符
    const SEPARATOR: &str = ":";
    /// 格式化字符串
    const FORMAT: LazyLock<String> = LazyLock::new(|| {
        format!(
            r#"{1}{0}{2}{0}{3}{0}{4}{0}{5}{0}{6}{0}{7}{0}{8}"#,
            Self::SEPARATOR,
            "#{client_tty}",
            "#{client_pid}",
            "#{client_name}",
            "#{client_session}",
            "#{session_id}",
            "#{session_name}",
            "#{window_id}",
            "#{window_name}"
        )
    });
}

/// 解析 display-message 单行输出为 TmuxPane
///
/// 输出格式为 `%1:1234:bash:/home/user`，使用 splitn 保证路径中含 `:` 时仍可正确解析。
fn parse_pane_output(output: &str) -> Option<TmuxPane> {
    let mut parts = output.splitn(4, PaneFormat::SEPARATOR);
    let pane_id = parts.next()?;
    let pane_pid = parts.next()?;
    let pane_current_command = parts.next()?;
    let pane_current_path = parts.next()?;
    Some(TmuxPane {
        pane_id: pane_id.strip_prefix('%')?.parse().ok()?,
        pane_pid: pane_pid.parse().ok()?,
        pane_current_command: pane_current_command.to_string(),
        pane_current_path: pane_current_path.to_string(),
    })
}

/// 解析 list-clients 单行输出为 TmuxClient
///
/// 输出格式为 `/dev/pts/1:1563:/dev/pts/1:main:$3:main:@3:bash`，
/// 其中 session_id 带 `$` 前缀、window_id 带 `@` 前缀（与 pane_id 的 `%` 前缀一致）。
fn parse_client_output(line: &str) -> Option<TmuxClient> {
    let mut parts = line.splitn(8, ClientFormat::SEPARATOR);
    let client_tty = parts.next()?;
    let client_pid = parts.next()?;
    let client_name = parts.next()?;
    let client_session = parts.next()?;
    let session_id = parts.next()?;
    let session_name = parts.next()?;
    let window_id = parts.next()?;
    let window_name = parts.next()?;
    Some(TmuxClient {
        client_tty: client_tty.to_string(),
        client_pid: client_pid.parse().ok()?,
        client_name: client_name.to_string(),
        client_session: client_session.to_string(),
        session_id: session_id.strip_prefix('$')?.parse().ok()?,
        session_name: session_name.to_string(),
        window_id: window_id.strip_prefix('@')?.parse().ok()?,
        window_name: window_name.to_string(),
    })
}

/// 执行 tmux display-message 查询目标的窗格参数（2s 超时）
///
/// 完整限定目标格式为：会话:窗口.窗格，如 '$0:@12.%14'
///
/// 也可以只指定会话或窗口，如 '$0' 或 '$0:@12', 这将查询该会话或窗口下的活跃窗格。
pub async fn query_tmux_pane(socket: Option<&str>, target: Option<String>) -> Option<TmuxPane> {
    let mut cmd = tokio::process::Command::new("tmux");
    if let Some(socket) = socket {
        cmd.arg("-S").arg(socket);
    }
    cmd.arg("display-message").arg("-p"); // -p 参数的作用是将输出内容打印到标准输出（stdout），而不是显示在 tmux 的状态栏上
    if let Some(t) = &target {
        cmd.arg("-t").arg(t);
    }
    #[cfg(windows)]
    cmd.arg("-F")
        .raw_arg(format!("'{}'", PaneFormat::FORMAT.as_str())); // 参数原样传递给 cygwin 程序
    #[cfg(not(windows))]
    cmd.arg("-F").arg(PaneFormat::FORMAT.as_str());
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());
    let out = tokio::time::timeout(std::time::Duration::from_secs(2), cmd.output())
        .await
        .ok()?
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let output = String::from_utf8_lossy(&out.stdout);
    parse_pane_output(output.trim())
}

/// 执行 tmux list-clients 查询所有客户端参数（2s 超时）
pub async fn query_tmux_clients(socket: Option<&str>) -> Vec<TmuxClient> {
    let mut cmd = tokio::process::Command::new("tmux");
    if let Some(socket) = socket {
        cmd.arg("-S").arg(socket);
    }
    cmd.arg("list-clients");
    #[cfg(windows)]
    cmd.arg("-F")
        .raw_arg(format!("'{}'", ClientFormat::FORMAT.as_str())); // 参数原样传递给 cygwin 程序
    #[cfg(not(windows))]
    cmd.arg("-F").arg(ClientFormat::FORMAT.as_str());
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());
    let out = match tokio::time::timeout(std::time::Duration::from_secs(2), cmd.output()).await {
        Ok(Ok(out)) if out.status.success() => out,
        _ => return Vec::new(),
    };
    let output = String::from_utf8_lossy(&out.stdout);
    output.lines().filter_map(parse_client_output).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_query_tmux() {
        let socket = None;
        let clients = query_tmux_clients(socket).await;
        let target = Some(clients[clients.len() - 1].window_target());
        let pane = query_tmux_pane(socket, target).await;
        dbg!(&clients);
        dbg!(&pane);
        assert!(clients.len() > 0);
        assert!(pane.is_some());
    }

    #[test]
    fn test_format() {
        assert_eq!(
            PaneFormat::FORMAT.as_str(),
            "#{pane_id}:#{pane_pid}:#{pane_current_command}:#{pane_current_path}"
        );
        assert_eq!(
            ClientFormat::FORMAT.as_str(),
            "#{client_tty}:#{client_pid}:#{client_name}:#{client_session}:#{session_id}:#{session_name}:#{window_id}:#{window_name}"
        );
    }

    #[test]
    fn test_parse_pane_output() {
        let pane = parse_pane_output("%1:1234:bash:/home/user").unwrap();
        assert_eq!(pane.pane_id, 1);
        assert_eq!(pane.pane_pid, 1234);
        assert_eq!(pane.pane_current_command, "bash");
        assert_eq!(pane.pane_current_path, "/home/user");
    }

    #[test]
    fn test_parse_pane_output_path_with_colon() {
        let pane = parse_pane_output("%12:99:zsh:/opt/a:b/c").unwrap();
        assert_eq!(pane.pane_id, 12);
        assert_eq!(pane.pane_current_path, "/opt/a:b/c");
    }

    #[test]
    fn test_parse_pane_output_invalid() {
        assert!(parse_pane_output("").is_none());
        assert!(parse_pane_output("%1:not_a_pid:bash:/tmp").is_none());
        assert!(parse_pane_output("1:1234:bash:/tmp").is_none()); // 缺少 % 前缀
    }

    #[test]
    fn test_parse_client_output() {
        // 依据真实 tmux 输出构造：session_id 带 $ 前缀，window_id 带 @ 前缀
        let client = parse_client_output("/dev/pty0:1563:/dev/pty0:3:$3:3:@3:bash").unwrap();
        assert_eq!(client.client_tty, "/dev/pty0");
        assert_eq!(client.client_pid, 1563);
        assert_eq!(client.client_name, "/dev/pty0");
        assert_eq!(client.client_session, "3");
        assert_eq!(client.session_id, 3);
        assert_eq!(client.session_name, "3");
        assert_eq!(client.window_id, 3);
        assert_eq!(client.window_name, "bash");
    }

    #[test]
    fn test_parse_client_output_invalid() {
        assert!(parse_client_output("").is_none());
        assert!(parse_client_output("/dev/pts/1:not_a_pid:name:session").is_none());
        // 字段数量不足（格式串共 8 个字段）
        assert!(parse_client_output("/dev/pts/1:1:name:session").is_none());
        // session_id 缺少 $ 前缀
        assert!(parse_client_output("/dev/pts/1:1:n:s:3:s:@3:w").is_none());
        // window_id 缺少 @ 前缀
        assert!(parse_client_output("/dev/pts/1:1:n:s:$3:s:3:w").is_none());
    }
}
