import { execFile, spawn, type ExecFileOptionsWithStringEncoding, type SpawnOptions } from "node:child_process"
import { promisify } from "node:util"

const execFileAsync = promisify(execFile)

/** Background work must never allocate a Windows console or invoke a shell. */
export function spawnBackground(file: string, args: string[], options: SpawnOptions) {
  return spawn(file, args, { ...options, windowsHide: true, shell: false })
}

export function execFileBackground(
  file: string,
  args: string[],
  options: ExecFileOptionsWithStringEncoding,
) {
  return execFileAsync(file, args, { ...options, windowsHide: true, shell: false })
}
