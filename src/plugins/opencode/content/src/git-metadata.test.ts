import { afterEach, describe, expect, it } from "bun:test"
import { execFileSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { gitMetadataForCwd, redactGitRemoteUrl } from "./git-metadata"

const tempDirs: string[] = []

afterEach(() => {
  while (tempDirs.length > 0) {
    rmSync(tempDirs.pop()!, { recursive: true, force: true })
  }
})

function makeGitRepo(): { dir: string; commit: string } {
  const dir = mkdtempSync(join(tmpdir(), "opencode-git-metadata-"))
  tempDirs.push(dir)
  execFileSync("git", ["init"], { cwd: dir, stdio: "ignore" })
  execFileSync("git", ["config", "user.email", "test@example.com"], { cwd: dir })
  execFileSync("git", ["config", "user.name", "Test User"], { cwd: dir })
  writeFileSync(join(dir, "README.md"), "hello\n")
  execFileSync("git", ["add", "README.md"], { cwd: dir })
  execFileSync("git", ["commit", "-m", "init"], { cwd: dir, stdio: "ignore" })
  execFileSync("git", ["branch", "-M", "main"], { cwd: dir })
  execFileSync("git", ["remote", "add", "origin", "https://token@github.com/acme/app.git"], {
    cwd: dir,
  })
  const commit = execFileSync("git", ["rev-parse", "HEAD"], { cwd: dir, encoding: "utf8" }).trim()
  return { dir, commit }
}

describe("git metadata", () => {
  it("redacts credentials from URL-style remotes", () => {
    expect(redactGitRemoteUrl("https://token@example.com/org/repo.git")).toBe(
      "https://example.com/org/repo.git",
    )
    expect(redactGitRemoteUrl("git@github.com:org/repo.git")).toBe("git@github.com:org/repo.git")
  })

  it("captures origin, branch, and commit", () => {
    const repo = makeGitRepo()

    expect(gitMetadataForCwd(repo.dir)).toEqual({
      git_origin_url: "https://github.com/acme/app.git",
      git_branch: "main",
      git_commit_sha: repo.commit,
    })
  })

  it("omits branch for detached HEAD", () => {
    const repo = makeGitRepo()
    execFileSync("git", ["checkout", "--detach", "HEAD"], { cwd: repo.dir, stdio: "ignore" })

    expect(gitMetadataForCwd(repo.dir)).toEqual({
      git_origin_url: "https://github.com/acme/app.git",
      git_commit_sha: repo.commit,
    })
  })

  it("omits all fields outside a git repository", () => {
    const dir = mkdtempSync(join(tmpdir(), "opencode-not-git-"))
    tempDirs.push(dir)

    expect(gitMetadataForCwd(dir)).toEqual({})
  })
})
