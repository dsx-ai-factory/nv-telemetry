// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::process::Command;

#[test]
fn strict_exit_status_reflects_observed_reports() {
    for (target, strict, count, expected, diagnostic) in [
        ("/redfish/v1/Chassis/1U/Sensors/CPU1Temp", true, "1", 0, ""),
        (
            "/redfish/v1/Sensors/CPU1Temp",
            true,
            "1",
            1,
            "projection issues",
        ),
        ("/redfish/v1/Sensors/CPU1Temp", false, "1", 0, ""),
        (
            "/redfish/v1/Chassis/1U/Sensors/CPU1Temp",
            true,
            "0",
            2,
            "greater than zero",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_nv-telemetry-probe"));
        command.args([
            "--mode",
            "mock",
            "--endpoint-id",
            "cli-test",
            "--sensor",
            target,
            "--count",
            count,
        ]);
        if strict {
            command.arg("--strict");
        }
        let result = command.output().expect("probe starts");
        let stderr = String::from_utf8(result.stderr).expect("UTF-8 output");
        assert_eq!(
            result.status.code(),
            Some(expected),
            "{target}, strict={strict}, count={count}: {stderr}"
        );
        assert!(stderr.contains(diagnostic), "{stderr}");
    }
}
