# Contributing guide

Thank you for considering contributing to **DFS**!  We welcome bug reports,
patches and documentation improvements.

---

## Getting started
1. **Fork** the repository and create a local clone.
2. Install prerequisites (`go 1.24`, `docker`, `protoc`).
3. Browse the issues, all issues should be well described and have tags.

---

### Discord Channel
I am happy to provide guidance, answer questions and help everyone willing to contribute. For that, join the [Discord Channel](https://discord.gg/WrZKdMv3Q6).

---

## Helpful commands for getting started (run from \dfs directory)
| Step | Command | Explanation |
|------|---------| ----------------- |
| Format | `make fmt` | formats your code properly | 
| Dev setup | `make dev-setup` | installs protoc |
| Clean proto | `make clean` | cleans protobuf files in pkg/proto |
| Generate proto | `make proto` | regenerates protobuf files according to definitions|
| Unit tests | `make test` | runs all unit tests |
| e2e tests | `make e2e` | runs all e2e tests (requires docker engine running) |

---

## Commits
* Use the conventional‐commits style: `scope: summary` – e.g. `feat: new feature/enchancement`.  
* Reference an issue when applicable: `Fixes #42`.
* Sign each commit.

---

## Pull requests - [PR template](.github/PULL_REQUEST_TEMPLATE.md)
1. Keep the PR focussed – one fix or feature per PR.
2. Update / add tests.
3. Update documentation (README or docs/*) when behaviour changes.
4. The PR description should explain *why* the change is needed.
5. If referencing any issue, add to the description.
6. Run all tests before submitting PR.
7. At least one maintainer review is required before merging.

---

## Code style
* Go 1.24, standard formatting (`go fmt`, `goimports`).
* Log using `pkg/logging` helpers; avoid `fmt.Println` in production code.
* Make sure functions are small and testable. 
* Dependency injection pattern is your friend.

---

## License / DCO
By contributing you agree that your code will be released under the MIT license and that you have the right to do so. 
