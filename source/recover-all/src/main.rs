use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks for reading

fn main() {
    let args: Vec<String> = env::args().collect();
    
    // Show usage if help is requested
    if args.len() > 1 && (args[1] == "-h" || args[1] == "--help") {
        println!("Usage: {}", args[0]);
        println!("This tool will interactively prompt for recovery parameters.");
        std::process::exit(0);
    }
    
    // Check if running as root
    if !is_root() {
        eprintln!("Warning: This tool works best when run as root for full disk access.");
        eprintln!("Some files may not be recoverable without root privileges.");
    }
    
    // Get target directory from user input
    let target_dir = get_target_directory_input();
    
    // Get partition from user input
    let partition = get_partition_input();
    
    // Get recovery directory from user input
    let recovery_dir = get_recovery_directory_input();
    
    // Get timespan from user input
    let timespan_hours = get_timespan_input();
    
    // Create recovery directory
    if let Err(e) = fs::create_dir_all(&recovery_dir) {
        eprintln!("Failed to create recovery directory: {}", e);
        std::process::exit(1);
    }
    
    println!("Target directory: {}", target_dir);
    println!("Partition: {}", partition);
    println!("Recovery directory: {}", recovery_dir);
    if let Some(hours) = timespan_hours {
        if hours > 0 {
            println!("Timespan: {} hours", hours);
        } else {
            println!("Timespan: All time");
        }
    } else {
        println!("Timespan: All time");
    }
    
    let cutoff_time = if let Some(hours) = timespan_hours {
        if hours > 0 {
            SystemTime::now() - Duration::from_secs(hours * 3600)
        } else {
            UNIX_EPOCH // Use epoch time for unlimited recovery
        }
    } else {
        UNIX_EPOCH // Use epoch time for unlimited recovery
    };
    
    // Step 1: Try to find recently deleted files using find command with ctime
    if timespan_hours.is_some() && timespan_hours.unwrap() > 0 {
        println!("\n=== Step 1: Searching for recently modified inodes ===");
        find_recent_changes(&target_dir, timespan_hours.unwrap());
    } else {
        println!("\n=== Step 1: Searching for all accessible inodes ===");
        find_all_files(&target_dir);
    }
    
    // Step 2: Search for file fragments in unallocated space
    println!("\n=== Step 2: Searching for file fragments ===");
    search_for_fragments(&target_dir, &recovery_dir, cutoff_time);
    
    // Step 3: Try to recover using debugfs if available
    println!("\n=== Step 3: Attempting debugfs recovery ===");
    if let Some(hours) = timespan_hours {
        if hours > 0 {
            attempt_debugfs_recovery_with_device(&partition, &recovery_dir, hours);
        } else {
            attempt_debugfs_recovery_unlimited_with_device(&partition, &recovery_dir);
        }
    } else {
        attempt_debugfs_recovery_unlimited_with_device(&partition, &recovery_dir);
    }
    
    // Step 4: Search for common file headers in raw disk data
    println!("\n=== Step 4: Searching for file signatures ===");
    search_file_signatures(&target_dir, &recovery_dir);
    
    // Step 5: Search partition directly for file signatures
    println!("\n=== Step 5: Searching partition directly for file signatures ===");
    search_partition_for_signatures(&partition, &recovery_dir);
    
    println!("\n=== Recovery attempt completed ===");
    println!("Check the recovery directory: {}", recovery_dir);
    println!("Note: File recovery success depends on disk usage since deletion.");
}

fn get_target_directory_input() -> String {
    loop {
        print!("Enter target directory to search for deleted files: ");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let target_dir = input.trim().to_string();
        
        if target_dir.is_empty() {
            println!("Please enter a valid directory path.");
            continue;
        }
        
        if !Path::new(&target_dir).exists() {
            println!("Directory does not exist: {}", target_dir);
            continue;
        }
        
        return target_dir;
    }
}

fn get_partition_input() -> String {
    loop {
        print!("Enter partition to recover deleted files: ");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let partition = input.trim().to_string();
        
        if partition.is_empty() {
            println!("Please enter a valid partition.");
            continue;
        }
        
        if !partition.starts_with("/dev/") {
            println!("Partition should start with /dev/ (e.g., /dev/sda1)");
            continue;
        }
        
        if !Path::new(&partition).exists() {
            println!("Partition {} does not exist. Please try again.", partition);
            continue;
        }
        
        return partition;
    }
}

fn get_recovery_directory_input() -> String {
    loop {
        print!("Enter recovery directory: ");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let recovery_dir = input.trim().to_string();
        
        if recovery_dir.is_empty() {
            println!("Please enter a valid directory path.");
            continue;
        }
        
        // Check if we can create the directory
        if let Err(e) = fs::create_dir_all(&recovery_dir) {
            println!("Cannot create directory {}: {}", recovery_dir, e);
            continue;
        }
        
        return recovery_dir;
    }
}

fn get_timespan_input() -> Option<u64> {
    loop {
        print!("Enter recovery timespan in hours (default all time): ");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let timespan_str = input.trim();
        
        if timespan_str.is_empty() {
            return None; // Default to all time
        }
        
        match timespan_str.parse::<u64>() {
            Ok(hours) => return Some(hours),
            Err(_) => {
                println!("Please enter a valid number of hours or press Enter for all time.");
                continue;
            }
        }
    }
}

fn is_root() -> bool {
    unsafe { libc::getuid() == 0 }
}

fn find_all_files(target_dir: &str) {
    println!("Searching for all accessible files and inodes...");
    
    // Find all files without time restrictions
    let output = Command::new("find")
        .arg(target_dir)
        .arg("-type")
        .arg("f")
        .arg("-ls")
        .output();
    
    match output {
        Ok(output) => {
            if !output.stdout.is_empty() {
                println!("All accessible files found:");
                println!("{}", String::from_utf8_lossy(&output.stdout));
            } else {
                println!("No accessible files found with find command.");
            }
        }
        Err(e) => println!("Failed to run find command: {}", e),
    }
    
    // Also try to find any hidden or temporary files that might be remnants
    let hidden_output = Command::new("find")
        .arg(target_dir)
        .arg("-name")
        .arg(".*")
        .arg("-type")
        .arg("f")
        .arg("-ls")
        .output();
    
    match hidden_output {
        Ok(output) => {
            if !output.stdout.is_empty() {
                println!("\nHidden files found:");
                println!("{}", String::from_utf8_lossy(&output.stdout));
            }
        }
        Err(e) => println!("Failed to search for hidden files: {}", e),
    }
}

fn find_recent_changes(target_dir: &str, hours: u64) {
    println!("Searching for recently changed files...");
    
    let output = Command::new("find")
        .arg(target_dir)
        .arg("-type")
        .arg("f")
        .arg("-ctime")
        .arg(format!("-{}", hours / 24 + 1)) // Convert to days, add 1 for safety
        .arg("-ls")
        .output();
    
    match output {
        Ok(output) => {
            if !output.stdout.is_empty() {
                println!("Recently changed files found:");
                println!("{}", String::from_utf8_lossy(&output.stdout));
            } else {
                println!("No recently changed files found with find command.");
            }
        }
        Err(e) => println!("Failed to run find command: {}", e),
    }
}

fn search_for_fragments(target_dir: &str, recovery_dir: &str, cutoff_time: SystemTime) {
    println!("Searching for file fragments in directory...");
    
    // Look for temporary files, recently modified files, etc.
    if let Ok(entries) = fs::read_dir(target_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    // Check if file was modified recently
                    if let Ok(modified) = metadata.modified() {
                        if modified > cutoff_time {
                            println!("Found file modified since cutoff: {:?}", path);
                            
                            // Try to recover if it looks like a temporary or backup file
                            if let Some(file_name) = path.file_name() {
                                let name = file_name.to_string_lossy();
                                if name.contains("tmp") || name.contains("temp") || 
                                   name.contains("backup") || name.contains("~") ||
                                   name.starts_with('.') {
                                    attempt_file_recovery(&path, recovery_dir);
                                }
                            }
                        } else if cutoff_time == UNIX_EPOCH {
                            // For unlimited recovery, check all potential recovery files
                            println!("Found potential recovery file: {:?}", path);
                            
                            if let Some(file_name) = path.file_name() {
                                let name = file_name.to_string_lossy();
                                if name.contains("tmp") || name.contains("temp") || 
                                   name.contains("backup") || name.contains("~") ||
                                   name.starts_with('.') || name.contains("lost+found") {
                                    attempt_file_recovery(&path, recovery_dir);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn attempt_file_recovery(source: &Path, recovery_dir: &str) {
    if let Some(file_name) = source.file_name() {
        let dest_path = format!("{}/{}", recovery_dir, file_name.to_string_lossy());
        
        match fs::copy(source, &dest_path) {
            Ok(_) => println!("Recovered file: {}", dest_path),
            Err(e) => println!("Failed to recover {:?}: {}", source, e),
        }
    }
}

fn attempt_debugfs_recovery_unlimited_with_device(device: &str, recovery_dir: &str) {
    println!("Attempting unlimited debugfs recovery on device: {}", device);
    attempt_debugfs_unlimited_on_device(device, recovery_dir);
}

fn attempt_debugfs_recovery_with_device(device: &str, recovery_dir: &str, _hours: u64) {
    println!("Attempting debugfs recovery on device: {}", device);
    attempt_debugfs_on_device(device, recovery_dir);
}

fn attempt_debugfs_unlimited_on_device(device: &str, recovery_dir: &str) {
    // Try to list all deleted inodes without time restrictions
    let output = Command::new("debugfs")
        .arg("-R")
        .arg("lsdel")
        .arg(device)
        .output();
    
    match output {
        Ok(output) => {
            if !output.stdout.is_empty() {
                println!("All deleted inodes found:");
                let lsdel_output = String::from_utf8_lossy(&output.stdout);
                println!("{}", lsdel_output);
                
                // Parse the output and try to recover all files
                for line in lsdel_output.lines() {
                    if let Some(inode) = extract_inode_from_lsdel_line(line) {
                        recover_inode_with_debugfs(device, &inode, recovery_dir);
                    }
                }
            } else {
                println!("No deleted inodes found or debugfs not available.");
            }
        }
        Err(e) => println!("debugfs not available or failed: {}", e),
    }
    
    // Also try to search for any unlinked but potentially recoverable inodes
    let unlinked_output = Command::new("debugfs")
        .arg("-R")
        .arg("undel")
        .arg(device)
        .output();
    
    match unlinked_output {
        Ok(output) => {
            if !output.stdout.is_empty() {
                println!("Unlinked inodes found:");
                let undel_output = String::from_utf8_lossy(&output.stdout);
                println!("{}", undel_output);
            }
        }
        Err(_) => {
            // undel command might not be available in all debugfs versions
            println!("undel command not available in debugfs");
        }
    }
}

fn attempt_debugfs_on_device(device: &str, recovery_dir: &str) {
    // Try to list recently deleted inodes
    let output = Command::new("debugfs")
        .arg("-R")
        .arg("lsdel")
        .arg(device)
        .output();
    
    match output {
        Ok(output) => {
            if !output.stdout.is_empty() {
                println!("Deleted inodes found:");
                let lsdel_output = String::from_utf8_lossy(&output.stdout);
                println!("{}", lsdel_output);
                
                // Parse the output and try to recover files
                for line in lsdel_output.lines() {
                    if let Some(inode) = extract_inode_from_lsdel_line(line) {
                        recover_inode_with_debugfs(device, &inode, recovery_dir);
                    }
                }
            } else {
                println!("No deleted inodes found or debugfs not available.");
            }
        }
        Err(e) => println!("debugfs not available or failed: {}", e),
    }
}

fn extract_inode_from_lsdel_line(line: &str) -> Option<String> {
    // Parse debugfs lsdel output format
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 {
        parts.get(0).map(|s| s.to_string())
    } else {
        None
    }
}

fn recover_inode_with_debugfs(device: &str, inode: &str, recovery_dir: &str) {
    let output_file = format!("{}/recovered_inode_{}", recovery_dir, inode);
    
    let output = Command::new("debugfs")
        .arg("-R")
        .arg(format!("dump <{}> {}", inode, output_file))
        .arg(device)
        .output();
    
    match output {
        Ok(_) => {
            if Path::new(&output_file).exists() {
                println!("Recovered inode {} to {}", inode, output_file);
            }
        }
        Err(e) => println!("Failed to recover inode {}: {}", inode, e),
    }
}

fn search_file_signatures(target_dir: &str, recovery_dir: &str) {
    println!("Searching for file signatures in directory...");
    
    // Common file signatures
    let signatures = get_file_signatures();
    
    if let Ok(entries) = fs::read_dir(target_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                search_signatures_in_file(&path, &signatures, recovery_dir);
            }
        }
    }
}

fn search_partition_for_signatures(device: &str, recovery_dir: &str) {
    println!("Searching partition {} for file signatures...", device);
    
    if !is_root() {
        println!("Warning: Partition scanning requires root privileges. Some signatures may not be found.");
    }
    
    let signatures = get_file_signatures();
    
    // Try to read directly from the partition device
    if let Ok(mut file) = fs::File::open(device) {
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut position = 0;
        let mut found_count = 0;
        
        println!("Scanning partition {} for file signatures...", device);
        
        while let Ok(bytes_read) = file.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }
            
            for (ext, signature) in &signatures {
                if let Some(sig_pos) = find_signature(&buffer[..bytes_read], signature) {
                    println!("Found {} signature at partition offset {}", ext, position + sig_pos);
                    found_count += 1;
                    
                    // Try to extract the file from the partition
                    if let Err(e) = extract_file_from_partition(
                        device, 
                        position + sig_pos, 
                        ext, 
                        recovery_dir,
                        found_count
                    ) {
                        println!("Failed to extract file from partition: {}", e);
                    }
                }
            }
            
            position += bytes_read;
            
            // Print progress every 100MB
            if position % (100 * 1024 * 1024) == 0 {
                println!("Scanned {} MB of partition...", position / (1024 * 1024));
            }
        }
        
        println!("Partition scan completed. Found {} potential files.", found_count);
    } else {
        println!("Failed to open partition device: {}. Root privileges may be required.", device);
    }
}

fn extract_file_from_partition(
    device: &str, 
    offset: usize, 
    extension: &str, 
    recovery_dir: &str,
    file_number: usize
) -> io::Result<()> {
    let output_file = format!("{}/partition_recovered_{}_{}.{}", recovery_dir, file_number, offset, extension);
    
    let mut source = fs::File::open(device)?;
    let mut dest = fs::File::create(&output_file)?;
    
    // Skip to the signature position
    let mut buffer = vec![0u8; offset];
    source.read_exact(&mut buffer)?;
    
    // Copy a reasonable amount of data from the signature position
    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut total_copied = 0;
    const MAX_FILE_SIZE: usize = 50 * 1024 * 1024; // 50MB max for partition recovery
    
    while total_copied < MAX_FILE_SIZE {
        match source.read(&mut buffer) {
            Ok(0) => break, // EOF
            Ok(bytes_read) => {
                dest.write_all(&buffer[..bytes_read])?;
                total_copied += bytes_read;
            }
            Err(e) => return Err(e),
        }
    }
    
    println!("Extracted potential {} file from partition: {}", extension, output_file);
    Ok(())
}

fn get_file_signatures() -> HashMap<&'static str, Vec<u8>> {
    let mut signatures = HashMap::new();
    
    // Common file signatures
    signatures.insert("jpg", vec![0xFF, 0xD8, 0xFF]);
    signatures.insert("png", vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]);
    signatures.insert("pdf", vec![0x25, 0x50, 0x44, 0x46]);
    signatures.insert("zip", vec![0x50, 0x4B, 0x03, 0x04]);
    signatures.insert("docx", vec![0x50, 0x4B, 0x03, 0x04]); // Actually ZIP-based
    signatures.insert("xlsx", vec![0x50, 0x4B, 0x03, 0x04]); // Actually ZIP-based
    signatures.insert("txt", vec![0x54, 0x68, 0x69, 0x73]); // "This" - common text start
    signatures.insert("mp3", vec![0xFF, 0xFB]);
    signatures.insert("mp4", vec![0x00, 0x00, 0x00, 0x20, 0x66, 0x74, 0x79, 0x70]);
    signatures.insert("exe", vec![0x4D, 0x5A]);
    signatures.insert("elf", vec![0x7F, 0x45, 0x4C, 0x46]);
    
    signatures
}

fn search_signatures_in_file(file_path: &Path, signatures: &HashMap<&str, Vec<u8>>, recovery_dir: &str) {
    if let Ok(mut file) = fs::File::open(file_path) {
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut position = 0;
        
        while let Ok(bytes_read) = file.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }
            
            for (ext, signature) in signatures {
                if let Some(sig_pos) = find_signature(&buffer[..bytes_read], signature) {
                    println!("Found {} signature in {:?} at position {}", ext, file_path, position + sig_pos);
                    
                    // Try to extract the file
                    if let Err(e) = extract_file_from_signature(
                        file_path, 
                        position + sig_pos, 
                        ext, 
                        recovery_dir
                    ) {
                        println!("Failed to extract file: {}", e);
                    }
                }
            }
            
            position += bytes_read;
        }
    }
}

fn find_signature(data: &[u8], signature: &[u8]) -> Option<usize> {
    if signature.len() > data.len() {
        return None;
    }
    
    for i in 0..=data.len() - signature.len() {
        if &data[i..i + signature.len()] == signature {
            return Some(i);
        }
    }
    None
}

fn extract_file_from_signature(
    source_file: &Path, 
    offset: usize, 
    extension: &str, 
    recovery_dir: &str
) -> io::Result<()> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let output_file = format!("{}/recovered_{}_{}.{}", recovery_dir, timestamp, offset, extension);
    
    let mut source = fs::File::open(source_file)?;
    let mut dest = fs::File::create(&output_file)?;
    
    // Skip to the signature position
    let mut buffer = vec![0u8; offset];
    source.read_exact(&mut buffer)?;
    
    // Copy the rest of the file (or a reasonable amount)
    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut total_copied = 0;
    const MAX_FILE_SIZE: usize = 100 * 1024 * 1024; // 100MB max
    
    while total_copied < MAX_FILE_SIZE {
        match source.read(&mut buffer) {
            Ok(0) => break, // EOF
            Ok(bytes_read) => {
                dest.write_all(&buffer[..bytes_read])?;
                total_copied += bytes_read;
            }
            Err(e) => return Err(e),
        }
    }
    
    println!("Extracted potential {} file: {}", extension, output_file);
    Ok(())
}

// External crate dependency simulation (in a real project, add libc to Cargo.toml)
#[allow(non_camel_case_types)]
mod libc {
    pub type uid_t = u32;
    unsafe extern "C" {
        pub fn getuid() -> uid_t;
    }
}
