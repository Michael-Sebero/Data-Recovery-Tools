use std::fs::{self, File};
use std::io::{self, Read, Write, BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

struct MemoryCopyTool {
    files_in_memory: HashMap<PathBuf, Vec<u8>>,
    chunk_size: usize,
    delay_between_chunks: Duration,
    max_memory_usage: usize,
}

impl MemoryCopyTool {
    fn new() -> Self {
        Self {
            files_in_memory: HashMap::new(),
            chunk_size: 64 * 1024,  // 64KB chunks - much smaller than before
            delay_between_chunks: Duration::from_millis(10),  // 10ms delay between chunks
            max_memory_usage: 256 * 1024 * 1024,  // 256MB max memory usage
        }
    }

    fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay_between_chunks = delay;
        self
    }

    fn with_max_memory(mut self, max_memory: usize) -> Self {
        self.max_memory_usage = max_memory;
        self
    }

    fn load_file_to_memory_chunked(&mut self, file_path: &Path) -> io::Result<()> {
        println!("Loading file to memory (chunked): {}", file_path.display());
        
        let file = File::open(file_path)?;
        let file_size = file.metadata()?.len() as usize;
        
        // If file is too large for our memory limit, we'll process it differently
        if file_size > self.max_memory_usage {
            println!("⚠️  File {} is too large ({} bytes) for memory buffering, will use streaming copy", 
                     file_path.display(), file_size);
            return Ok(());
        }

        let mut reader = BufReader::new(file);
        let mut buffer = Vec::with_capacity(file_size);
        let mut chunk = vec![0u8; self.chunk_size];
        
        loop {
            let bytes_read = reader.read(&mut chunk)?;
            if bytes_read == 0 {
                break;
            }
            
            buffer.extend_from_slice(&chunk[..bytes_read]);
            
            // Add delay to reduce disk stress
            thread::sleep(self.delay_between_chunks);
            
            // Check if we're approaching memory limits
            if self.get_memory_usage() + buffer.len() > self.max_memory_usage {
                println!("⚠️  Approaching memory limit, will use streaming for remaining files");
                break;
            }
        }
        
        self.files_in_memory.insert(file_path.to_path_buf(), buffer);
        
        println!("✓ Loaded {} bytes into memory", self.files_in_memory[file_path].len());
        Ok(())
    }

    fn copy_file_streaming(&self, source_path: &Path, dest_path: &Path) -> io::Result<()> {
        println!("Streaming copy: {} -> {}", source_path.display(), dest_path.display());
        
        // Create parent directories if they don't exist
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let source_file = File::open(source_path)?;
        let dest_file = File::create(dest_path)?;
        
        let mut reader = BufReader::new(source_file);
        let mut writer = BufWriter::new(dest_file);
        
        let mut buffer = vec![0u8; self.chunk_size];
        let mut total_bytes = 0;
        
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            
            writer.write_all(&buffer[..bytes_read])?;
            total_bytes += bytes_read;
            
            // Add delay to reduce disk stress
            thread::sleep(self.delay_between_chunks);
            
            // Periodically flush to avoid large write buffers
            if total_bytes % (self.chunk_size * 10) == 0 {
                writer.flush()?;
            }
        }
        
        writer.flush()?;
        println!("✓ Streamed {} bytes to {}", total_bytes, dest_path.display());
        Ok(())
    }

    fn load_directory_to_memory(&mut self, dir_path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut file_paths = Vec::new();
        
        fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        collect_files(&path, files)?;
                    } else {
                        files.push(path);
                    }
                }
            }
            Ok(())
        }

        if dir_path.is_file() {
            file_paths.push(dir_path.to_path_buf());
        } else {
            collect_files(dir_path, &mut file_paths)?;
        }

        println!("Found {} files to copy", file_paths.len());

        // Sort files by size (smaller files first to maximize memory usage efficiency)
        file_paths.sort_by_key(|path| {
            path.metadata().map(|m| m.len()).unwrap_or(0)
        });

        // Load smaller files into memory first
        for file_path in &file_paths {
            // Check file size before loading
            if let Ok(metadata) = file_path.metadata() {
                let file_size = metadata.len() as usize;
                
                // Only load small files into memory
                if file_size <= self.max_memory_usage / 4 && 
                   self.get_memory_usage() + file_size <= self.max_memory_usage {
                    match self.load_file_to_memory_chunked(file_path) {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("⚠️  Failed to load {}: {}", file_path.display(), e);
                        }
                    }
                } else {
                    println!("📁 File {} ({} bytes) will be streamed", file_path.display(), file_size);
                }
            }
        }

        Ok(file_paths)
    }

    fn write_from_memory(&self, original_path: &Path, dest_path: &Path) -> io::Result<()> {
        if let Some(data) = self.files_in_memory.get(original_path) {
            // Create parent directories if they don't exist
            if let Some(parent) = dest_path.parent() {
                fs::create_dir_all(parent)?;
            }

            println!("Writing from memory: {} -> {}", original_path.display(), dest_path.display());
            
            let dest_file = File::create(dest_path)?;
            let mut writer = BufWriter::new(dest_file);
            
            // Write in chunks to avoid large single writes
            for chunk in data.chunks(self.chunk_size) {
                writer.write_all(chunk)?;
                thread::sleep(self.delay_between_chunks);
            }
            
            writer.flush()?;
            
            println!("✓ Written {} bytes to {}", data.len(), dest_path.display());
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("File not found in memory: {}", original_path.display())
            ))
        }
    }

    fn copy_to_destination(&self, source_root: &Path, dest_root: &Path, file_paths: &[PathBuf]) -> io::Result<()> {
        println!("\n=== Starting copy operation ===");
        
        for file_path in file_paths {
            // Calculate relative path from source root
            let relative_path = if source_root.is_file() {
                // If source is a file, use just the filename
                PathBuf::from(file_path.file_name().unwrap())
            } else {
                // If source is a directory, preserve directory structure
                file_path.strip_prefix(source_root)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                    .to_path_buf()
            };

            let dest_path = dest_root.join(relative_path);
            
            // Try memory copy first, fall back to streaming
            if self.files_in_memory.contains_key(file_path) {
                match self.write_from_memory(file_path, &dest_path) {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("⚠️  Failed to write from memory {}: {}", dest_path.display(), e);
                        eprintln!("    Falling back to streaming copy...");
                        
                        match self.copy_file_streaming(file_path, &dest_path) {
                            Ok(_) => {},
                            Err(e) => {
                                eprintln!("⚠️  Failed to stream {}: {}", dest_path.display(), e);
                            }
                        }
                    }
                }
            } else {
                // File not in memory, use streaming copy
                match self.copy_file_streaming(file_path, &dest_path) {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("⚠️  Failed to stream {}: {}", dest_path.display(), e);
                    }
                }
            }
        }

        Ok(())
    }

    fn get_memory_usage(&self) -> usize {
        self.files_in_memory.values().map(|v| v.len()).sum()
    }
}

fn get_user_input(prompt: &str) -> String {
    print!("{}", prompt);
    io::stdout().flush().unwrap();
    
    let stdin = io::stdin();
    let mut input = String::new();
    stdin.lock().read_line(&mut input).unwrap();
    input.trim().to_string()
}

fn get_path_from_user(prompt: &str) -> PathBuf {
    loop {
        let input = get_user_input(prompt);
        
        // Handle empty input
        if input.is_empty() {
            println!("Please enter a valid path.");
            continue;
        }
        
        // Expand tilde (~) to home directory
        let expanded_path = if input.starts_with("~/") {
            if let Some(home_dir) = dirs::home_dir() {
                home_dir.join(&input[2..])
            } else {
                PathBuf::from(input)
            }
        } else if input == "~" {
            if let Some(home_dir) = dirs::home_dir() {
                home_dir
            } else {
                PathBuf::from(input)
            }
        } else {
            PathBuf::from(input)
        };
        
        return expanded_path;
    }
}

fn get_settings_from_user() -> (usize, Duration, usize) {
    println!("\n=== Advanced Settings (press Enter for defaults) ===");
    
    let chunk_size = {
        let input = get_user_input("Chunk size in KB (default: 64): ");
        if input.is_empty() {
            64 * 1024
        } else {
            input.parse::<usize>().unwrap_or(64) * 1024
        }
    };
    
    let delay = {
        let input = get_user_input("Delay between chunks in ms (default: 10): ");
        if input.is_empty() {
            Duration::from_millis(10)
        } else {
            Duration::from_millis(input.parse::<u64>().unwrap_or(10))
        }
    };
    
    let max_memory = {
        let input = get_user_input("Maximum memory usage in MB (default: 256): ");
        if input.is_empty() {
            256 * 1024 * 1024
        } else {
            input.parse::<usize>().unwrap_or(256) * 1024 * 1024
        }
    };
    
    (chunk_size, delay, max_memory)
}

fn main() {

    // Ask if user wants to configure advanced settings
    let use_advanced = get_user_input("Configure advanced settings? (y/N): ");
    let (chunk_size, delay, max_memory) = if use_advanced.to_lowercase().starts_with('y') {
        get_settings_from_user()
    } else {
        (64 * 1024, Duration::from_millis(10), 256 * 1024 * 1024)
    };
    
    // Get source path from user
    let source_path = loop {
        let path = get_path_from_user("Enter source path (file or directory): ");
        
        if path.exists() {
            break path;
        } else {
            println!("❌ Path does not exist: {}", path.display());
            println!("Please try again.\n");
        }
    };
    
    // Get destination path from user
    let dest_path = loop {
        let path = get_path_from_user("Enter destination path: ");
        
        // Check if destination parent directory exists or can be created
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                match fs::create_dir_all(parent) {
                    Ok(_) => {
                        println!("✓ Created destination directory: {}", parent.display());
                        break path;
                    }
                    Err(e) => {
                        println!("❌ Cannot create destination directory: {}", e);
                        println!("Please try again.\n");
                        continue;
                    }
                }
            } else {
                break path;
            }
        } else {
            break path;
        }
    };
    
    // Confirm the operation
    println!("\n=== Operation Summary ===");
    println!("Source: {}", source_path.display());
    println!("Destination: {}", dest_path.display());
    println!("Chunk size: {} KB", chunk_size / 1024);
    println!("Delay between chunks: {} ms", delay.as_millis());
    println!("Max memory usage: {} MB", max_memory / 1024 / 1024);
    
    let file_or_dir = if source_path.is_file() { "file" } else { "directory" };
    println!("This will gently copy the {} and all its contents to the destination.", file_or_dir);
    
    let confirmation = get_user_input("\nProceed with copy operation? (y/N): ");
    if !confirmation.to_lowercase().starts_with('y') {
        println!("Operation cancelled.");
        return;
    }

    // Initialize the memory copy tool with custom settings
    let mut copy_tool = MemoryCopyTool::new()
        .with_chunk_size(chunk_size)
        .with_delay(delay)
        .with_max_memory(max_memory);

    println!("\n=== Starting Gentle Copy Process ===");
    println!("Source: {}", source_path.display());
    println!("Destination: {}", dest_path.display());
    println!();

    // Phase 1: Load smaller files into memory, prepare for streaming larger ones
    println!("Phase 1: Analyzing and loading smaller files into memory...");
    let file_paths = match copy_tool.load_directory_to_memory(&source_path) {
        Ok(paths) => paths,
        Err(e) => {
            eprintln!("Error loading files to memory: {}", e);
            std::process::exit(1);
        }
    };

    let memory_usage = copy_tool.get_memory_usage();
    println!("Memory usage: {:.2} MB", memory_usage as f64 / 1024.0 / 1024.0);

    // Phase 2: Copy files (from memory or streaming)
    println!("\nPhase 2: Copying files (mix of memory and streaming)...");
    if let Err(e) = copy_tool.copy_to_destination(&source_path, &dest_path, &file_paths) {
        eprintln!("Error during copy operation: {}", e);
        std::process::exit(1);
    }

    println!("\n=== Copy operation completed successfully! ===");
    println!("Copied {} files", file_paths.len());
    println!("Total memory used: {:.2} MB", memory_usage as f64 / 1024.0 / 1024.0);
    println!("Drive stress minimized through chunked processing and delays.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_memory_copy_tool_chunked() {
        let temp_dir = tempdir().unwrap();
        let source_file = temp_dir.path().join("test.txt");
        let dest_dir = temp_dir.path().join("destination");
        
        // Create a test file
        fs::write(&source_file, "Hello, World!").unwrap();
        
        let mut copy_tool = MemoryCopyTool::new();
        
        // Test loading file to memory
        assert!(copy_tool.load_file_to_memory_chunked(&source_file).is_ok());
        assert_eq!(copy_tool.files_in_memory.len(), 1);
        
        // Test writing from memory
        fs::create_dir_all(&dest_dir).unwrap();
        let dest_file = dest_dir.join("test.txt");
        assert!(copy_tool.write_from_memory(&source_file, &dest_file).is_ok());
        
        // Verify the copied file
        let copied_content = fs::read_to_string(&dest_file).unwrap();
        assert_eq!(copied_content, "Hello, World!");
    }

    #[test]
    fn test_streaming_copy() {
        let temp_dir = tempdir().unwrap();
        let source_file = temp_dir.path().join("large_test.txt");
        let dest_file = temp_dir.path().join("large_test_copy.txt");
        
        // Create a larger test file
        let test_content = "A".repeat(1024 * 1024); // 1MB file
        fs::write(&source_file, &test_content).unwrap();
        
        let copy_tool = MemoryCopyTool::new();
        
        // Test streaming copy
        assert!(copy_tool.copy_file_streaming(&source_file, &dest_file).is_ok());
        
        // Verify the copied file
        let copied_content = fs::read_to_string(&dest_file).unwrap();
        assert_eq!(copied_content, test_content);
    }
}
