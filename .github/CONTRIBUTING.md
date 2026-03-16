# Contributing to KislayPHP Core

Thank you for your interest in contributing to KislayPHP Core! This document provides guidelines and information for contributors.

## 🚀 Quick Start

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/core.git`
3. Create a feature branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Run tests: `php run-tests.php`
6. Commit your changes: `git commit -m "Add your feature"`
7. Push to your fork: `git push origin feature/your-feature-name`
8. Create a Pull Request

## 📋 Development Setup

### Prerequisites

- PHP 8.2+
- C++ compiler (GCC 9+ or Clang 11+)
- TLS library development libraries
- zlib development libraries
- HTTP client library development libraries

### Building from Source

```bash
phpize
./configure
make
make install
```

### Running Tests

```bash
php run-tests.php
```

## 🐛 Reporting Bugs

When reporting bugs, please include:

- PHP version: `php --version`
- Operating system and version
- Steps to reproduce the issue
- Expected vs actual behavior
- Any relevant error messages or stack traces

## 💡 Feature Requests

We welcome feature requests! Please:

- Check if the feature has already been requested
- Provide a clear description of the feature
- Explain the use case and why it would be valuable
- Consider how it fits with the existing architecture

## 📝 Code Style

- Follow PSR-12 for PHP code
- Use C++17 features
- Write clear, descriptive commit messages
- Include tests for new functionality
- Update documentation as needed

## 🔒 Security

If you discover a security vulnerability, please email security@kislayphp.com instead of creating a public issue.

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.

## 🙏 Recognition

Contributors will be recognized in the project's contributor list and release notes.

Thank you for helping make KislayPHP better! 🎉