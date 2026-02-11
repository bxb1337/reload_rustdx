# AGENTS.md - Development Guidelines for reload_rustdx

This file provides coding standards and development guidelines for agentic coding agents working in this repository.

## Project Overview

- **Language**: Rust (edition 2024)
- **Type**: CLI application (uses clap)
- **Dependencies**: polars for data processing
- **Structure**: Standard Cargo project

## Build & Development Commands

### Core Commands
```bash
# Build the project
cargo build

# Build for release
cargo build --release

# Run the application
cargo run

# Run with arguments
cargo run -- [args]

# Check compilation without building
cargo check
```

### Testing Commands
```bash
# Run all tests
cargo test

# Run a single test (replace test_name)
cargo test test_name

# Run tests with specific filter pattern
cargo test filter_pattern

# Run tests with output
cargo test -- --nocapture

# Run specific test file
cargo test --lib path::to::module
```

### Code Quality Commands
```bash
# Format code (uses rustfmt defaults)
cargo fmt

# Check formatting without applying changes
cargo fmt -- --check

# Run Clippy lints
cargo clippy

# Run Clippy with all pedantic lints
cargo clippy -- -D warnings -W clippy::pedantic

# Check for unused dependencies
cargo machete
```

### Development Workflow
```bash
# Update dependencies
cargo update

# Check for outdated dependencies
cargo outdated

# Generate documentation
cargo doc --open

# Analyze build time
cargo build --timings
```

## Code Style Guidelines

### Naming Conventions
- **Functions/Variables**: `snake_case` for functions and variables
- **Types**: `PascalCase` for structs, enums, and type aliases
- **Constants**: `SCREAMING_SNAKE_CASE` for const and static items
- **Modules**: `snake_case` for module names and file names

### Import Organization
```rust
// Standard library imports first
use std::collections::HashMap;
use std::fs;

// External crates second (grouped by crate)
use clap::Parser;
use polars::prelude::*;

// Local modules last
mod utils;
use utils::helper_function;
```

### Error Handling Patterns
```rust
// Use Result<T, Box<dyn std::error::Error>> for main() and CLI apps
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // application logic
    Ok(())
}

// For libraries, use specific error types
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Parse error: {0}")]
    Parse(String),
}
```

### Function Documentation
```rust
/// Brief description of what the function does.
///
/// # Arguments
/// 
/// * `param1` - Description of the first parameter
/// * `param2` - Description of the second parameter
///
/// # Returns
/// 
/// Description of the return value
///
/// # Examples
///
/// ```
/// let result = function_name(arg1, arg2);
/// assert_eq!(result, expected_value);
/// ```
pub fn function_name(param1: Type1, param2: Type2) -> ReturnType {
    // implementation
}
```

### Code Formatting
- Use `rustfmt` with default settings
- Line length: 100 characters (rustfmt default)
- Use trailing commas in multi-line structs/arrays
- Prefer match over if-let-else for complex pattern matching

## Module Organization

```
src/
├── main.rs          # Application entry point and CLI setup
├── lib.rs           # Library interface (if this is also a library)
├── cli/             # CLI-related modules
│   ├── mod.rs
│   ├── args.rs      # Command-line argument definitions
│   └── commands.rs  # Command implementations
├── core/            # Core business logic
│   ├── mod.rs
│   └── processor.rs # Main processing logic
├── utils/           # Utility functions
│   ├── mod.rs
│   └── helpers.rs
└── tests/           # Integration tests
    └── integration_test.rs
```

## Testing Guidelines

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_name() {
        // Arrange
        let input = "test_input";
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert_eq!(result, "expected_output");
    }
}
```

### Integration Tests
- Place in `tests/` directory
- Test the public API of your library/crate
- Use realistic test data when possible

## Dependencies Management

### Adding Dependencies
```bash
# Add runtime dependency
cargo add crate_name

# Add dev dependency
cargo add --dev crate_name

# Add with specific features
cargo add crate_name --features "feature1,feature2"
```

### Recommended Dependencies
- **CLI**: `clap` (already included)
- **Error handling**: `thiserror` for custom errors, `anyhow` for application errors
- **Logging**: `tracing` + `tracing-subscriber`
- **Testing**: `proptest` for property-based testing
- **Async**: `tokio` if async functionality is needed

## Git Workflow

### Commit Message Format
```
type(scope): brief description

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

### Pre-commit Checks
- `cargo fmt -- --check` - ensure code is formatted
- `cargo clippy` - ensure no lint warnings
- `cargo test` - ensure all tests pass

## Performance Guidelines

### Profiling Commands
```bash
# Install profiling tools
cargo install cargo-flamegraph
cargo install cargo-profiler

# Generate flamegraph
cargo flamegraph --bin binary_name

# Memory profiling
cargo profiler --callgrind
```

### Optimization Tips
- Use `#[inline]` for small, hot functions
- Consider `Vec::with_capacity()` when size is known
- Use `Cow<str>` for optional string allocations
- Profile before optimizing

## Security Considerations

- Validate all external input
- Use secure defaults for file permissions
- Be careful with `unsafe` blocks (document why they're necessary)
- Keep dependencies updated: `cargo audit`

## Environment Variables

```bash
# Enable debug logging
RUST_LOG=debug

# Enable backtraces
RUST_BACKTRACE=1

# Enable full backtraces
RUST_BACKTRACE=full
```

## Debugging Tips

```bash
# Run with debugger
rust-gdb target/debug/binary_name

# Run with RR (record and replay)
rr record target/debug/binary_name
rr replay

# Memory debugging with valgrind
valgrind --leak-check=full target/debug/binary_name
```

## Development Requirements
- Do not code very complex logic in any function. When necessary, break down into smaller functions.
- Do not use global variables unless absolutely necessary.
- Do not use 'unsafe' blocks unless absolutely necessary.
