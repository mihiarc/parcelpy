# Changelog

All notable changes to ParcelPy will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Performance benchmarking infrastructure
- Enhanced error handling for CLI commands
- Configuration management system

### Changed
- Improved test coverage reporting
- Updated documentation structure

### Fixed
- Import path consistency across modules

## [0.1.0] - 2025-01-XX (Alpha Release)

### Added
- **Core Features**
  - PostgreSQL + PostGIS database management
  - Parcel data ingestion and processing
  - Spatial queries and analytics
  - Market analytics engine
  - Risk assessment tools
  - Census data integration via SocialMapper
  - Command-line interfaces for all operations

- **Testing Infrastructure**
  - Modern pytest-based testing (258 tests)
  - Comprehensive fixture library with 15+ fixtures
  - Test markers for different test types (unit, integration, performance)
  - Mock framework for external dependencies
  - Coverage reporting (HTML + XML + terminal)

- **Visualization Components**
  - Enhanced parcel visualizer
  - Database integration for visualizations
  - Interactive mapping capabilities
  - Data loading and processing utilities

- **Earth Engine Integration**
  - Parcel partitioning and processing
  - Remote sensing data integration
  - County-level data processing

- **Streamlit Application**
  - Web-based data exploration interface
  - Interactive components for database operations
  - Map-based visualizations
  - Data upload and processing tools

- **Development Tools**
  - Modern package structure (no `src/` directory)
  - Clean import paths
  - Comprehensive configuration management
  - Development documentation and roadmap

### Technical Details
- **Dependencies**: PostgreSQL 13+, PostGIS 3.0+, Python 3.11+
- **Package Management**: UV-based dependency management
- **Testing**: pytest with extensive mocking and fixtures
- **Coverage**: 8% initial coverage with expansion roadmap
- **Architecture**: Modular design with clear separation of concerns

### Documentation
- Comprehensive README with quick start guide
- Development roadmap with clear priorities
- API documentation structure
- Installation and configuration guides

### Known Limitations
- CLI test coverage needs expansion (0-13% currently)
- Streamlit components need testing infrastructure
- Some visualization modules need additional test coverage
- External API integrations need more robust error handling

### Migration Notes
- This is the initial alpha release
- No breaking changes (initial release)
- All imports use `from parcelpy.module import ...` pattern 