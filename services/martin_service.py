import subprocess


def restart_martin():
    # 'reload' will invoke ExecReload (TERM => restart behavior); 'restart' is fine too.
    # NOTE: no --user, no sudo
    cmd = ["systemctl", "--user", "restart", "martin.service"]

    print('cmd: ', cmd)
    try:
        out = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print('out: ', out)
        print('out.stdout: ', out.stdout)

        return {"ok": True, "stdout": out.stdout}
    except subprocess.CalledProcessError as e:
        print({"ok": False, "stdout": e.stdout,
              "stderr": e.stderr, "code": e.returncode})
        return {"ok": False, "stdout": e.stdout, "stderr": e.stderr, "code": e.returncode}
