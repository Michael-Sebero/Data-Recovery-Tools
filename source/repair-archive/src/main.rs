use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::Path;

#[derive(Debug, Clone)]
pub enum ArchiveType {
    Zip,
    Tar,
    Gzip,
    Bzip2,
    SevenZip,
    Rar,
    Unknown,
}

#[derive(Debug)]
pub enum RepairStrategy {
    FixEof,
    RepairCentralDirectory,
    RecoverPartialData,
    FixChecksums,
    RebuildIndex,
    TruncateCorrupted,
}

#[derive(Debug)]
pub struct ArchiveRecovery {
    pub file_path: String,
    pub archive_type: ArchiveType,
    pub backup_created: bool,
    pub repairs_attempted: Vec<RepairStrategy>,
    pub success: bool,
    pub recovered_files: usize,
    pub total_files: usize,
}

pub struct UniversalArchiveRecoverer {
    verbose: bool,
    create_backup: bool,
    max_recovery_attempts: usize,
}

impl UniversalArchiveRecoverer {
    pub fn new() -> Self {
        Self {
            verbose: false,
            create_backup: true,
            max_recovery_attempts: 5,
        }
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn with_backup(mut self, create_backup: bool) -> Self {
        self.create_backup = create_backup;
        self
    }

    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_recovery_attempts = max_attempts;
        self
    }

    pub fn recover_archive(&self, file_path: &str) -> io::Result<ArchiveRecovery> {
        let mut recovery = ArchiveRecovery {
            file_path: file_path.to_string(),
            archive_type: ArchiveType::Unknown,
            backup_created: false,
            repairs_attempted: Vec::new(),
            success: false,
            recovered_files: 0,
            total_files: 0,
        };

        if self.verbose {
            println!("Starting recovery for: {}", file_path);
        }

        // Create backup if requested
        if self.create_backup {
            self.create_backup_file(file_path)?;
            recovery.backup_created = true;
            if self.verbose {
                println!("Backup created: {}.backup", file_path);
            }
        }

        // Detect archive type
        recovery.archive_type = self.detect_archive_type(file_path)?;
        if self.verbose {
            println!("Detected archive type: {:?}", recovery.archive_type);
        }

        // Attempt repairs based on archive type
        match recovery.archive_type {
            ArchiveType::Zip => self.repair_zip_archive(file_path, &mut recovery)?,
            ArchiveType::Tar => self.repair_tar_archive(file_path, &mut recovery)?,
            ArchiveType::Gzip => self.repair_gzip_archive(file_path, &mut recovery)?,
            ArchiveType::Bzip2 => self.repair_bzip2_archive(file_path, &mut recovery)?,
            ArchiveType::SevenZip => self.repair_7z_archive(file_path, &mut recovery)?,
            ArchiveType::Rar => self.repair_rar_archive(file_path, &mut recovery)?,
            ArchiveType::Unknown => {
                if self.verbose {
                    println!("Unknown archive type, attempting generic recovery");
                }
                self.generic_recovery(file_path, &mut recovery)?;
            }
        }

        Ok(recovery)
    }

    fn detect_archive_type(&self, file_path: &str) -> io::Result<ArchiveType> {
        let mut file = File::open(file_path)?;
        let mut buffer = [0u8; 16];
        file.read_exact(&mut buffer)?;

        // Check magic bytes
        if &buffer[0..4] == b"PK\x03\x04" || &buffer[0..4] == b"PK\x05\x06" {
            return Ok(ArchiveType::Zip);
        }
        if &buffer[0..3] == b"\x1f\x8b\x08" {
            return Ok(ArchiveType::Gzip);
        }
        if &buffer[0..3] == b"BZh" {
            return Ok(ArchiveType::Bzip2);
        }
        if &buffer[0..6] == b"7z\xbc\xaf\x27\x1c" {
            return Ok(ArchiveType::SevenZip);
        }
        if &buffer[0..4] == b"Rar!" {
            return Ok(ArchiveType::Rar);
        }

        // Check for TAR (more complex detection)
        file.seek(SeekFrom::Start(257))?;
        let mut tar_buffer = [0u8; 5];
        if file.read_exact(&mut tar_buffer).is_ok() {
            if &tar_buffer == b"ustar" {
                return Ok(ArchiveType::Tar);
            }
        }

        // Check file extension as fallback
        if let Some(ext) = Path::new(file_path).extension() {
            match ext.to_str() {
                Some("zip") => return Ok(ArchiveType::Zip),
                Some("tar") => return Ok(ArchiveType::Tar),
                Some("gz") | Some("gzip") => return Ok(ArchiveType::Gzip),
                Some("bz2") | Some("bzip2") => return Ok(ArchiveType::Bzip2),
                Some("7z") => return Ok(ArchiveType::SevenZip),
                Some("rar") => return Ok(ArchiveType::Rar),
                _ => {}
            }
        }

        Ok(ArchiveType::Unknown)
    }

    fn repair_zip_archive(&self, file_path: &str, recovery: &mut ArchiveRecovery) -> io::Result<()> {
        if self.verbose {
            println!("Attempting ZIP archive repair...");
        }

        // Strategy 1: Fix missing End of Central Directory (EOCD)
        if self.fix_zip_eocd(file_path)? {
            recovery.repairs_attempted.push(RepairStrategy::FixEof);
            if self.verbose {
                println!("Fixed missing EOCD record");
            }
        }

        // Strategy 2: Repair central directory
        if self.repair_zip_central_directory(file_path)? {
            recovery.repairs_attempted.push(RepairStrategy::RepairCentralDirectory);
            if self.verbose {
                println!("Repaired central directory");
            }
        }

        // Strategy 3: Recover partial data
        let recovered = self.recover_zip_partial_data(file_path)?;
        recovery.recovered_files = recovered;
        if recovered > 0 {
            recovery.repairs_attempted.push(RepairStrategy::RecoverPartialData);
            if self.verbose {
                println!("Recovered {} files from partial data", recovered);
            }
        }

        recovery.success = !recovery.repairs_attempted.is_empty();
        Ok(())
    }

    fn fix_zip_eocd(&self, file_path: &str) -> io::Result<bool> {
        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
        let file_size = file.metadata()?.len();
        
        // Look for EOCD signature in the last 64KB
        let search_size = std::cmp::min(file_size, 65536);
        file.seek(SeekFrom::End(-(search_size as i64)))?;
        
        let mut buffer = vec![0u8; search_size as usize];
        file.read_exact(&mut buffer)?;
        
        // Search for EOCD signature (PK\x05\x06)
        for i in (0..buffer.len().saturating_sub(4)).rev() {
            if &buffer[i..i+4] == b"PK\x05\x06" {
                // Found EOCD, check if it's valid
                if i + 22 <= buffer.len() {
                    return Ok(false); // Already has valid EOCD
                }
            }
        }
        
        // No EOCD found, create minimal one
        let eocd = vec![
            0x50, 0x4b, 0x05, 0x06, // EOCD signature
            0x00, 0x00, // Disk number
            0x00, 0x00, // Disk with central directory
            0x00, 0x00, // Number of entries on this disk
            0x00, 0x00, // Total number of entries
            0x00, 0x00, 0x00, 0x00, // Size of central directory
            0x00, 0x00, 0x00, 0x00, // Offset of central directory
            0x00, 0x00, // Comment length
        ];
        
        file.seek(SeekFrom::End(0))?;
        file.write_all(&eocd)?;
        file.flush()?;
        
        Ok(true)
    }

    fn repair_zip_central_directory(&self, file_path: &str) -> io::Result<bool> {
        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let mut local_files = Vec::new();
        let mut pos = 0;
        
        // Find all local file headers
        while pos < buffer.len().saturating_sub(4) {
            if &buffer[pos..pos+4] == b"PK\x03\x04" {
                if let Some(file_info) = self.parse_local_file_header(&buffer[pos..]) {
                    pos += file_info.total_size;
                    local_files.push((pos - file_info.total_size, file_info));
                } else {
                    pos += 1;
                }
            } else {
                pos += 1;
            }
        }
        
        if local_files.is_empty() {
            return Ok(false);
        }
        
        // Build central directory
        let mut central_dir = Vec::new();
        let cd_offset = buffer.len();
        
        for (local_offset, file_info) in &local_files {
            let cd_entry = self.create_central_directory_entry(file_info, *local_offset as u32);
            central_dir.extend(cd_entry);
        }
        
        // Update EOCD or create new one
        let eocd = self.create_eocd_record(local_files.len() as u16, central_dir.len() as u32, cd_offset as u32);
        
        // Write central directory and EOCD
        file.seek(SeekFrom::Start(cd_offset as u64))?;
        file.write_all(&central_dir)?;
        file.write_all(&eocd)?;
        file.flush()?;
        
        Ok(true)
    }

    fn recover_zip_partial_data(&self, file_path: &str) -> io::Result<usize> {
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let mut recovered = 0;
        let mut pos = 0;
        
        while pos < buffer.len().saturating_sub(4) {
            if &buffer[pos..pos+4] == b"PK\x03\x04" {
                if let Some(file_info) = self.parse_local_file_header(&buffer[pos..]) {
                    // Try to extract the file data
                    let data_start = pos + 30 + file_info.filename_length as usize + file_info.extra_field_length as usize;
                    if data_start < buffer.len() {
                        let data_end = std::cmp::min(data_start + file_info.compressed_size as usize, buffer.len());
                        
                        if data_end > data_start {
                            // Save recovered file
                            let output_path = format!("{}.recovered_{}", file_path, recovered);
                            let mut output_file = File::create(&output_path)?;
                            output_file.write_all(&buffer[data_start..data_end])?;
                            recovered += 1;
                        }
                    }
                    pos += file_info.total_size;
                } else {
                    pos += 1;
                }
            } else {
                pos += 1;
            }
        }
        
        Ok(recovered)
    }

    fn repair_tar_archive(&self, _file_path: &str, recovery: &mut ArchiveRecovery) -> io::Result<()> {
        if self.verbose {
            println!("Attempting TAR archive repair...");
        }

        // TAR repair strategies
        if self.fix_tar_truncation(_file_path)? {
            recovery.repairs_attempted.push(RepairStrategy::TruncateCorrupted);
        }

        if self.rebuild_tar_index(_file_path)? {
            recovery.repairs_attempted.push(RepairStrategy::RebuildIndex);
        }

        recovery.success = !recovery.repairs_attempted.is_empty();
        Ok(())
    }

    fn repair_gzip_archive(&self, _file_path: &str, recovery: &mut ArchiveRecovery) -> io::Result<()> {
        if self.verbose {
            println!("Attempting GZIP archive repair...");
        }

        // GZIP repair strategies
        if self.fix_gzip_footer(_file_path)? {
            recovery.repairs_attempted.push(RepairStrategy::FixEof);
        }

        if self.fix_gzip_checksums(_file_path)? {
            recovery.repairs_attempted.push(RepairStrategy::FixChecksums);
        }

        recovery.success = !recovery.repairs_attempted.is_empty();
        Ok(())
    }

    fn repair_bzip2_archive(&self, _file_path: &str, recovery: &mut ArchiveRecovery) -> io::Result<()> {
        if self.verbose {
            println!("Attempting BZIP2 archive repair...");
        }

        // BZIP2 repair strategies - similar to GZIP
        recovery.success = false;
        Ok(())
    }

    fn repair_7z_archive(&self, _file_path: &str, recovery: &mut ArchiveRecovery) -> io::Result<()> {
        if self.verbose {
            println!("Attempting 7Z archive repair...");
        }

        // 7Z repair strategies
        recovery.success = false;
        Ok(())
    }

    fn repair_rar_archive(&self, _file_path: &str, recovery: &mut ArchiveRecovery) -> io::Result<()> {
        if self.verbose {
            println!("Attempting RAR archive repair...");
        }

        // RAR repair strategies
        recovery.success = false;
        Ok(())
    }

    fn generic_recovery(&self, file_path: &str, recovery: &mut ArchiveRecovery) -> io::Result<()> {
        if self.verbose {
            println!("Attempting generic recovery...");
        }

        // Generic strategies that might work for any archive
        if self.fix_file_truncation(file_path)? {
            recovery.repairs_attempted.push(RepairStrategy::TruncateCorrupted);
        }

        recovery.success = !recovery.repairs_attempted.is_empty();
        Ok(())
    }

    // Helper methods
    fn create_backup_file(&self, file_path: &str) -> io::Result<()> {
        let backup_path = format!("{}.backup", file_path);
        std::fs::copy(file_path, backup_path)?;
        Ok(())
    }

    fn parse_local_file_header(&self, data: &[u8]) -> Option<LocalFileInfo> {
        if data.len() < 30 {
            return None;
        }

        let compressed_size = u32::from_le_bytes([data[18], data[19], data[20], data[21]]);
        let filename_length = u16::from_le_bytes([data[26], data[27]]);
        let extra_field_length = u16::from_le_bytes([data[28], data[29]]);

        Some(LocalFileInfo {
            compressed_size,
            filename_length,
            extra_field_length,
            total_size: 30 + filename_length as usize + extra_field_length as usize + compressed_size as usize,
        })
    }

    fn create_central_directory_entry(&self, file_info: &LocalFileInfo, local_offset: u32) -> Vec<u8> {
        let mut entry = vec![0x50, 0x4b, 0x01, 0x02]; // Central directory signature
        entry.extend_from_slice(&[0x14, 0x00]); // Version made by
        entry.extend_from_slice(&[0x14, 0x00]); // Version needed
        entry.extend_from_slice(&[0x00, 0x00]); // Flags
        entry.extend_from_slice(&[0x00, 0x00]); // Compression method
        entry.extend_from_slice(&[0x00, 0x00]); // Last mod time
        entry.extend_from_slice(&[0x00, 0x00]); // Last mod date
        entry.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // CRC32
        entry.extend_from_slice(&file_info.compressed_size.to_le_bytes()); // Compressed size
        entry.extend_from_slice(&file_info.compressed_size.to_le_bytes()); // Uncompressed size
        entry.extend_from_slice(&file_info.filename_length.to_le_bytes()); // Filename length
        entry.extend_from_slice(&[0x00, 0x00]); // Extra field length
        entry.extend_from_slice(&[0x00, 0x00]); // Comment length
        entry.extend_from_slice(&[0x00, 0x00]); // Disk number
        entry.extend_from_slice(&[0x00, 0x00]); // Internal attributes
        entry.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // External attributes
        entry.extend_from_slice(&local_offset.to_le_bytes()); // Local header offset
        
        entry
    }

    fn create_eocd_record(&self, num_entries: u16, cd_size: u32, cd_offset: u32) -> Vec<u8> {
        let mut eocd = vec![0x50, 0x4b, 0x05, 0x06]; // EOCD signature
        eocd.extend_from_slice(&[0x00, 0x00]); // Disk number
        eocd.extend_from_slice(&[0x00, 0x00]); // Disk with central directory
        eocd.extend_from_slice(&num_entries.to_le_bytes()); // Entries on this disk
        eocd.extend_from_slice(&num_entries.to_le_bytes()); // Total entries
        eocd.extend_from_slice(&cd_size.to_le_bytes()); // Central directory size
        eocd.extend_from_slice(&cd_offset.to_le_bytes()); // Central directory offset
        eocd.extend_from_slice(&[0x00, 0x00]); // Comment length
        eocd
    }

    fn fix_tar_truncation(&self, _file_path: &str) -> io::Result<bool> {
        // Implement TAR truncation fix
        Ok(false)
    }

    fn rebuild_tar_index(&self, _file_path: &str) -> io::Result<bool> {
        // Implement TAR index rebuild
        Ok(false)
    }

    fn fix_gzip_footer(&self, _file_path: &str) -> io::Result<bool> {
        // Implement GZIP footer fix
        Ok(false)
    }

    fn fix_gzip_checksums(&self, _file_path: &str) -> io::Result<bool> {
        // Implement GZIP checksum fix
        Ok(false)
    }

    fn fix_file_truncation(&self, _file_path: &str) -> io::Result<bool> {
        // Implement generic file truncation fix
        Ok(false)
    }
}

#[derive(Debug, Clone)]
struct LocalFileInfo {
    compressed_size: u32,
    filename_length: u16,
    extra_field_length: u16,
    total_size: usize,
}

// CLI interface
fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    
    let mut file_path = String::new();
    let mut verbose = false;
    let mut no_backup = false;
    
    // Parse arguments
    if args.len() > 1 {
        file_path = args[1].clone();
        verbose = args.contains(&"--verbose".to_string());
        no_backup = args.contains(&"--no-backup".to_string());
    }
    
    // Interactive mode if no file provided
    if file_path.is_empty() {
        
        loop {
            print!("Enter the path to the archive file (or 'quit' to exit): ");
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();
            
            if input.is_empty() {
                continue;
            }
            
            if input.to_lowercase() == "quit" || input.to_lowercase() == "exit" {
                println!("Goodbye!");
                return Ok(());
            }
            
            // Check if file exists
            if !Path::new(input).exists() {
                println!("Error: File '{}' does not exist. Please try again.", input);
                continue;
            }
            
            file_path = input.to_string();
            break;
        }
        
        // Ask for options
        print!("Enable verbose output? (y/N): ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        verbose = input.trim().to_lowercase().starts_with('y');
        
        print!("Skip backup creation? (y/N): ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        no_backup = input.trim().to_lowercase().starts_with('y');
        
        println!();
    }
    
    // Validate file path
    if !Path::new(&file_path).exists() {
        eprintln!("Error: File '{}' does not exist.", file_path);
        eprintln!("Usage: {} <archive_file> [--verbose] [--no-backup]", args[0]);
        std::process::exit(1);
    }
    
    let recoverer = UniversalArchiveRecoverer::new()
        .with_verbose(verbose)
        .with_backup(!no_backup);
    
    println!("Starting recovery process...");
    println!("File: {}", file_path);
    println!("Verbose: {}", verbose);
    println!("Create backup: {}", !no_backup);
    println!();
    
    match recoverer.recover_archive(&file_path) {
        Ok(recovery) => {
            println!("=== Recovery Results ===");
            println!("File: {}", recovery.file_path);
            println!("Archive type: {:?}", recovery.archive_type);
            println!("Success: {}", if recovery.success { "✓ Yes" } else { "✗ No" });
            println!("Repairs attempted: {:?}", recovery.repairs_attempted);
            println!("Files recovered: {}", recovery.recovered_files);
            
            if recovery.backup_created {
                println!("Backup created: {}.backup", recovery.file_path);
            }
            
            if recovery.success {
                println!("\n✓ Archive recovery completed successfully!");
            } else {
                println!("\n⚠ Archive recovery failed or no repairs were needed.");
            }
        }
        Err(e) => {
            eprintln!("✗ Recovery failed: {}", e);
            std::process::exit(1);
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_archive_detection() {
        let recoverer = UniversalArchiveRecoverer::new();
        
        // Create test ZIP file
        let zip_data = b"PK\x03\x04\x14\x00\x00\x00\x08\x00";
        fs::write("test.zip", zip_data).unwrap();
        
        let archive_type = recoverer.detect_archive_type("test.zip").unwrap();
        assert!(matches!(archive_type, ArchiveType::Zip));
        
        fs::remove_file("test.zip").unwrap();
    }

    #[test]
    fn test_backup_creation() {
        let recoverer = UniversalArchiveRecoverer::new();
        
        // Create test file
        fs::write("test_backup.txt", "test content").unwrap();
        
        recoverer.create_backup_file("test_backup.txt").unwrap();
        
        assert!(fs::metadata("test_backup.txt.backup").is_ok());
        
        fs::remove_file("test_backup.txt").unwrap();
        fs::remove_file("test_backup.txt.backup").unwrap();
    }

    #[test]
    fn test_recovery_result() {
        let recoverer = UniversalArchiveRecoverer::new().with_backup(false);
        
        // Create minimal test file
        fs::write("test_recovery.unknown", "some data").unwrap();
        
        let result = recoverer.recover_archive("test_recovery.unknown");
        assert!(result.is_ok());
        
        let recovery = result.unwrap();
        assert_eq!(recovery.file_path, "test_recovery.unknown");
        assert!(matches!(recovery.archive_type, ArchiveType::Unknown));
        
        fs::remove_file("test_recovery.unknown").unwrap();
    }
}
