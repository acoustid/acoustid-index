const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    const httpz = b.dependency("httpz", .{
        .target = target,
        .optimize = optimize,
    });

    const metrics = b.dependency("metrics", .{
        .target = target,
        .optimize = optimize,
    });

    const zul = b.dependency("zul", .{
        .target = target,
        .optimize = optimize,
    });

    const msgpack = b.addModule("msgpack", .{
        .root_source_file = b.path("lib/msgpack/msgpack.zig"),
        .target = target,
        .optimize = optimize,
    });

    const main_exe = b.addExecutable(.{
        .name = "fpindex",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    main_exe.root_module.addImport("httpz", httpz.module("httpz"));
    main_exe.root_module.addImport("metrics", metrics.module("metrics"));
    main_exe.root_module.addImport("zul", zul.module("zul"));
    main_exe.root_module.addImport("msgpack", msgpack);

    b.installArtifact(main_exe);

    const run_cmd = b.addRunArtifact(main_exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const main_tests = b.addTest(.{
        .name = "aindex-tests",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    main_tests.root_module.addImport("httpz", httpz.module("httpz"));
    main_tests.root_module.addImport("metrics", metrics.module("metrics"));
    main_tests.root_module.addImport("zul", zul.module("zul"));
    main_tests.root_module.addImport("msgpack", msgpack);

    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_main_tests.step);
}
