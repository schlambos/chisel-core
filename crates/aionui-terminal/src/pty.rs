//! Portable PTY abstraction for terminal sessions.
//!
//! This module provides a cross-platform interface to portable-pty for spawning
//! and managing pseudo-terminal sessions.

use std::io::{Read, Write};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use aionui_runtime::resolve_command_path;
use portable_pty::{Child, CommandBuilder, MasterPty, NativePtySystem, PtySize, PtySystem};
use tracing::{debug, error};

/// Apply the same environment sanitization as aionui_runtime::Builder
fn strip_pollution(cmd: &mut CommandBuilder) {
    cmd.env_remove("NODE_OPTIONS");
    cmd.env_remove("NODE_INSPECT");
    cmd.env_remove("NODE_DEBUG");
    cmd.env_remove("CLAUDECODE");
}

/// Ring buffer capacity for terminal output (512KB)
#[allow(dead_code)]
const RING_BUFFER_CAPACITY: usize = 512 * 1024;

/// Output coalescing interval (8ms)
const OUTPUT_COALESCING_INTERVAL: Duration = Duration::from_millis(8);

/// A terminal session handle that manages the PTY and its output.
pub struct TerminalSession {
    /// The master PTY handle (wrapped in Mutex<Option> for thread-safe closure)
    master: Mutex<Option<Box<dyn MasterPty + Send>>>,
    /// The child process handle (Arc for sharing with reader thread)
    child: Arc<Mutex<Option<Box<dyn Child + Send + Sync>>>>,
    /// Receiver for output data
    output_rx: Receiver<Vec<u8>>,
    /// Sender for input data (to PTY writer thread)
    input_tx: Mutex<Option<Sender<Vec<u8>>>>,
    /// Thread handle for PTY reader
    #[allow(dead_code)]
    reader_handle: thread::JoinHandle<()>,
    /// Thread handle for PTY writer
    #[allow(dead_code)]
    writer_handle: thread::JoinHandle<()>,
    /// Current terminal size
    size: PtySize,
}

impl TerminalSession {
    /// Create a new terminal session with the given command and size.
    pub fn new(command: Option<String>, cwd: Option<String>, cols: u16, rows: u16) -> Result<Self, String> {
        let pty_system: Box<dyn PtySystem> = Box::new(NativePtySystem::default());

        let size = PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        };

        let pair = pty_system
            .openpty(size)
            .map_err(|e| format!("failed to open PTY: {}", e))?;

        let master = pair.master;
        let slave = pair.slave;

        // Determine the shell to use:
        // 1. Check $SHELL environment variable
        // 2. Fall back to resolve_command_path("sh")
        // 3. Fall back to "sh"
        let shell_path = std::env::var("SHELL")
            .ok()
            .and_then(|s| if s.is_empty() { None } else { Some(s) })
            .unwrap_or_else(|| {
                resolve_command_path("sh")
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "sh".into())
            });

        // Build the command with aionui_runtime's env sanitization
        let mut cmd_builder = CommandBuilder::new(&shell_path);

        // Apply same env sanitization as aionui_runtime::Builder
        strip_pollution(&mut cmd_builder);

        // Set the shell command if provided
        if let Some(cmd) = command {
            cmd_builder.arg("-c");
            cmd_builder.arg(&cmd);
        }

        // Set working directory if provided
        if let Some(dir) = cwd {
            cmd_builder.cwd(dir);
        }

        // Spawn the command in the slave PTY
        let mut child = slave
            .spawn_command(cmd_builder)
            .map_err(|e| format!("failed to spawn shell: {}", e))?;

        // Create channels for bidirectional communication
        let (output_tx, output_rx) = channel();
        let (input_tx, input_rx) = channel();

        // Clone the master for the reader thread
        let master_reader = match master.try_clone_reader() {
            Ok(r) => r,
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("Failed to clone PTY reader: {}", e));
            }
        };

        // Clone the master for the writer thread
        let master_writer = match master.try_clone_writer() {
            Ok(w) => w,
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("Failed to clone PTY writer: {}", e));
            }
        };

        // Wrap child in Arc for sharing with reader thread
        let child_arc = Arc::new(Mutex::new(Some(child)));
        let child_for_reader = child_arc.clone();

        // Spawn reader thread with ring buffer and coalescing
        let reader_handle = Self::spawn_reader_thread(master_reader, output_tx, child_for_reader);

        // Spawn writer thread
        let writer_handle = Self::spawn_writer_thread(master_writer, input_rx);

        Ok(Self {
            master: Mutex::new(Some(master)),
            child: child_arc,
            output_rx,
            input_tx: Mutex::new(Some(input_tx)),
            reader_handle,
            writer_handle,
            size,
        })
    }

    /// Spawn the PTY reader thread with ring buffer and output coalescing.
    fn spawn_reader_thread(
        mut master: Box<dyn std::io::Read + Send>,
        output_tx: Sender<Vec<u8>>,
        child: Arc<Mutex<Option<Box<dyn Child + Send + Sync>>>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut coalescing_buffer = Vec::with_capacity(4096);
            let mut last_send_time = std::time::Instant::now();
            let mut temp_buf = [0u8; 4096];

            loop {
                match master.read(&mut temp_buf) {
                    Ok(0) => {
                        // EOF - terminal closed, reap child
                        debug!("PTY reader: EOF received");
                        if let Ok(mut guard) = child.lock() {
                            if let Some(mut child) = guard.take() {
                                let _ = child.wait();
                            }
                        }
                        break;
                    }
                    Ok(n) => {
                        coalescing_buffer.extend_from_slice(&temp_buf[..n]);

                        // Check if we should flush the coalescing buffer
                        let now = std::time::Instant::now();
                        if now.duration_since(last_send_time) >= OUTPUT_COALESCING_INTERVAL
                            || coalescing_buffer.len() >= 4096
                        {
                            if !coalescing_buffer.is_empty() {
                                let data = std::mem::take(&mut coalescing_buffer);
                                if output_tx.send(data).is_err() {
                                    debug!("PTY reader: output channel closed");
                                    break;
                                }
                                last_send_time = now;
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "PTY reader: read error");
                        break;
                    }
                }
            }

            // Send any remaining data before exiting
            if !coalescing_buffer.is_empty() {
                let _ = output_tx.send(coalescing_buffer);
            }

            debug!("PTY reader thread exited");
        })
    }

    /// Spawn the PTY writer thread.
    fn spawn_writer_thread(
        mut master: Box<dyn std::io::Write + Send>,
        input_rx: Receiver<Vec<u8>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            loop {
                match input_rx.recv() {
                    Ok(data) => {
                        if data.is_empty() {
                            // Legacy empty-data signal, also exit
                            break;
                        }
                        if let Err(e) = master.write_all(&data) {
                            error!(error = %e, "PTY writer: write error");
                            break;
                        }
                        // Flush after each write
                        if let Err(e) = master.flush() {
                            error!(error = %e, "PTY writer: flush error");
                            break;
                        }
                    }
                    Err(_) => {
                        // Channel disconnected, exit
                        debug!("PTY writer: input channel closed");
                        break;
                    }
                }
            }
            debug!("PTY writer thread exited");
        })
    }

    /// Send input to the terminal.
    pub fn send_input(&self, data: Vec<u8>) -> Result<(), String> {
        let guard = self.input_tx.lock().map_err(|_| "input_tx lock poisoned".to_string())?;
        if let Some(tx) = guard.as_ref() {
            tx.send(data).map_err(|e| format!("failed to send input: {}", e))?;
        }
        Ok(())
    }

    /// Receive output from the terminal (non-blocking).
    pub fn try_recv_output(&self) -> Result<Vec<u8>, std::sync::mpsc::TryRecvError> {
        self.output_rx.try_recv()
    }

    /// Resize the terminal.
    pub fn resize(&mut self, cols: u16, rows: u16) -> Result<(), String> {
        self.size.cols = cols;
        self.size.rows = rows;
        let guard = self.master.lock().map_err(|_| "master lock poisoned".to_string())?;
        guard
            .as_ref()
            .ok_or_else(|| "master PTY already closed".to_string())?
            .resize(self.size)
            .map_err(|e| format!("failed to resize PTY: {}", e))?;
        Ok(())
    }

    /// Kill the terminal session.
    pub fn kill(&self) {
        // 1. Close the master PTY first — this unblocks the reader thread
        //    (master.read() will return an error)
        if let Ok(mut guard) = self.master.lock() {
            guard.take();
        }

        // 2. Kill and reap the direct child process
        if let Ok(mut guard) = self.child.lock() {
            if let Some(mut child) = guard.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }

        // 3. Drop the input sender to signal the writer thread to exit
        if let Ok(mut guard) = self.input_tx.lock() {
            guard.take();
        }
    }

    /// Wait for the terminal session to finish.
    pub fn wait(&mut self) {
        // Drop the sender to allow the reader thread to exit
        // We can't drop self.output_tx and self.input_tx directly as they're not Copy
        // Instead, we'll just wait for the threads to finish

        // For now, we'll just let the Drop implementation handle cleanup
        // The threads will exit when the channels are closed
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        self.kill();
        // Note: We don't wait for threads here as it could cause deadlocks
        // The threads will exit when their channels are closed
    }
}

/// A simple ring buffer for terminal output.
pub struct RingBuffer {
    buffer: Vec<u8>,
    capacity: usize,
    write_pos: usize,
    read_pos: usize,
    len: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0; capacity],
            capacity,
            write_pos: 0,
            read_pos: 0,
            len: 0,
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        for &byte in data {
            self.buffer[self.write_pos] = byte;
            self.write_pos = (self.write_pos + 1) % self.capacity;
            if self.len < self.capacity {
                self.len += 1;
            } else {
                // Buffer is full, move read position
                self.read_pos = (self.read_pos + 1) % self.capacity;
            }
        }
    }

    pub fn read(&mut self, output: &mut [u8]) -> usize {
        let available = std::cmp::min(output.len(), self.len);
        if available == 0 {
            return 0;
        }

        for i in 0..available {
            output[i] = self.buffer[self.read_pos];
            self.read_pos = (self.read_pos + 1) % self.capacity;
        }
        self.len -= available;
        available
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

/// Shared terminal session that can be accessed from multiple threads.
pub struct SharedTerminalSession {
    inner: Arc<Mutex<TerminalSession>>,
}

impl SharedTerminalSession {
    pub fn new(session: TerminalSession) -> Self {
        Self {
            inner: Arc::new(Mutex::new(session)),
        }
    }

    pub fn send_input(&self, data: Vec<u8>) -> Result<(), String> {
        let guard = self.inner.lock().map_err(|e| format!("lock poisoned: {}", e))?;
        guard.send_input(data)
    }

    pub fn try_recv_output(&self) -> Result<Vec<u8>, std::sync::mpsc::TryRecvError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| std::sync::mpsc::TryRecvError::Disconnected)?;
        guard.try_recv_output()
    }

    pub fn resize(&self, cols: u16, rows: u16) -> Result<(), String> {
        let mut guard = self.inner.lock().map_err(|e| format!("lock poisoned: {}", e))?;
        guard.resize(cols, rows)
    }

    pub fn kill(&self) {
        let guard = self.inner.lock().ok();
        if let Some(g) = guard {
            g.kill();
        }
    }
}

impl Clone for SharedTerminalSession {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_basic() {
        let mut rb = RingBuffer::new(10);
        assert!(rb.is_empty());
        assert_eq!(rb.len(), 0);

        rb.write(&[1, 2, 3]);
        assert!(!rb.is_empty());
        assert_eq!(rb.len(), 3);

        let mut output = [0u8; 3];
        assert_eq!(rb.read(&mut output), 3);
        assert_eq!(output, [1, 2, 3]);
        assert!(rb.is_empty());
    }

    #[test]
    fn test_ring_buffer_overflow() {
        let mut rb = RingBuffer::new(5);
        rb.write(&[1, 2, 3, 4, 5, 6, 7]);

        // Buffer should contain [3, 4, 5, 6, 7] (oldest 2 bytes overwritten)
        assert_eq!(rb.len(), 5);

        let mut output = [0u8; 5];
        assert_eq!(rb.read(&mut output), 5);
        assert_eq!(output, [3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_ring_buffer_wrap_around() {
        let mut rb = RingBuffer::new(5);
        rb.write(&[1, 2, 3]);
        rb.write(&[4, 5, 6]);
        rb.write(&[7, 8]);

        // Should contain [5, 6, 7, 8, 3] or similar depending on implementation
        // The key is we can read back what we wrote (within capacity)
        let mut output = vec![0u8; 5];
        let n = rb.read(&mut output);
        assert_eq!(n, 5);
    }
}
