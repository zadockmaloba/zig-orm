const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addModule("zigorm", .{
        .root_source_file = b.path("src/database.zig"),
        .target = target,
        .optimize = optimize,
    });

    const postgres = b.dependency("libpq", .{
        .target = target,
        .optimize = optimize,
        .ssl = .None,
        //.@"disable-ssl" = true,
    });
    const libpq = postgres.artifact("pq");
    lib.linkLibrary(libpq);

    b.installArtifact(libpq);

    lib.addIncludePath(b.path("zig-out/include/"));
    lib.addIncludePath(b.path("zig-out/include/libpq/"));

    const lib_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/database.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_unit_tests.linkLibC();
    lib_unit_tests.linkLibrary(libpq);

    lib_unit_tests.addIncludePath(b.path("zig-out/include/"));
    lib_unit_tests.addIncludePath(b.path("zig-out/include/libpq/"));

    lib_unit_tests.root_module.addImport("zigorm", lib);

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}
