const initialBuild = Bun.spawnSync(["bun", "run", "ui:build"], {
  cwd: process.cwd(),
  env: { ...process.env },
  stdin: "inherit",
  stdout: "inherit",
  stderr: "inherit"
});

if (initialBuild.exitCode !== 0) {
  console.error(
    `[dev] Frontend build failed with code ${initialBuild.exitCode}. Aborting dev startup.`
  );
  process.exit(initialBuild.exitCode ?? 1);
}

const processes = [
  {
    name: "ui",
    cmd: ["bun", "run", "ui:dev"],
    env: { ...process.env }
  },
  {
    name: "api",
    cmd: ["bun", "run", "dev:api"],
    env: { ...process.env }
  }
] as const;

const running = processes.map((entry) => ({
  ...entry,
  proc: Bun.spawn(entry.cmd, {
    cwd: process.cwd(),
    env: entry.env,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit"
  })
}));

let shuttingDown = false;

const shutdown = (exitCode = 0): never => {
  if (shuttingDown) process.exit(exitCode);
  shuttingDown = true;
  for (const entry of running) {
    try {
      if (entry.proc.exitCode === null) entry.proc.kill();
    } catch {
      // Ignore process teardown errors during shutdown.
    }
  }
  process.exit(exitCode);
};

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));

const winner = await Promise.race(
  running.map(async (entry) => ({
    name: entry.name,
    exitCode: await entry.proc.exited
  }))
);

if (!shuttingDown) {
  const exitCode = Number.isFinite(winner.exitCode) ? winner.exitCode : 1;
  console.error(`[dev] ${winner.name} exited with code ${exitCode}. Stopping dev session.`);
  shutdown(exitCode);
}
