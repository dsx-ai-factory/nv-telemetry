# Contributing to nv-telemetry

## Making Changes

### Pull Request Flow

1. **Fork the repository** on GitHub

2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR-USERNAME/nv-telemetry.git
   cd nv-telemetry
   ```

3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/NVIDIA/nv-telemetry.git
   ```

4. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

5. **Make your changes** and commit with sign-off:
   ```bash
   git add .
   git commit -s -m "Your commit message"
   ```

   Run `make all` before pushing; it is the same gate CI runs. Use `make fmt`
   rather than `cargo fmt`, because the import layout relies on a nightly-only
   rustfmt option that stable rustfmt ignores with a warning instead of an
   error. `make rust-install` fetches the toolchain that step needs.

   A change under `spec/` also needs `make spec`, which checks the specs and
   draws again the traces the tests replay; until it has run, `cargo test`
   fails. It needs Node and Java. See [docs/FORMAL.md](docs/FORMAL.md).
6. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create Pull Request** on GitHub:
   - https://help.github.com/en/articles/about-pull-requests

8. **Once approved**, maintainers will merge your PR

## Signing Your Work

We require that all contributors "sign-off" on their commits. This certifies that the contribution is your original work, or you have rights to submit it under the same license, or a compatible license.

### How to Sign Off

Any contribution which contains commits that are not signed-off will not be accepted.

To sign off on a commit, use the `--signoff` (or `-s`) option when committing:

```bash
git commit -s -m "Add cool feature"
```

This will append the following to your commit message:

```
Signed-off-by: Your Name <your@email.com>
```

### Developer Certificate of Origin (DCO)

By signing off, you certify the following:

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

### Amending Commits

If you forgot to sign off on a commit:

```bash
# For the last commit
git commit --amend --signoff

# For multiple commits
git rebase --signoff HEAD~3  # Last 3 commits
```

## Review Process

### What to Expect

1. **Automated Checks**: CI/CD will run tests, linting, and formatting checks
2. **Code Review**: Maintainers will review your code for:
   - Correctness and functionality
   - Code quality and style
   - Test coverage
   - Documentation completeness
3. **Feedback**: Address review comments promptly
4. **Approval**: At least one maintainer approval required
5. **Merge**: Maintainers will merge once approved

## License

By contributing to nv-telemetry, you agree that your contributions will be licensed under the same license as the project (see LICENSE file).