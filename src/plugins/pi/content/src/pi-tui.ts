type PiTui = typeof import("@earendil-works/pi-tui");
type HyperlinkHelpers = Pick<PiTui, "getCapabilities" | "hyperlink">;

/** Pi's UI helpers are optional for a tracing extension and unavailable in Oh My Pi. */
export async function loadPiHyperlinkHelpers(
  host: string | undefined,
): Promise<Partial<HyperlinkHelpers>> {
  if (host === "@oh-my-pi/pi-coding-agent") return {};
  try {
    // Resolve only in the Pi UI path. Oh My Pi rejects package imports of
    // the old TUI at extension load time, including literal dynamic imports.
    const url = import.meta.resolve("@earendil-works/pi-tui");
    return (await import(url)) as PiTui;
  } catch {
    // Some hosts supply the extension API without Pi's optional UI helpers.
    return {};
  }
}
