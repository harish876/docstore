from conan import ConanFile
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get, rmdir
import os

class DocstoreConan(ConanFile):
    name = "docstore"
    version = "0.0.1"
    license = "BSD-3-Clause"
    homepage = "https://github.com/harish876/docstore"
    description = "Fork of LevelDB with document store support"
    topics = ("leveldb", "database", "document-store")
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_snappy": [True, False],
        "with_crc32c": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_snappy": True,
        "with_crc32c": True,
    }

    def export_sources(self):
        # Add any patch files or additional sources here if needed
        pass

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        if self.options.with_snappy:
            self.requires("snappy/1.1.10")
        if self.options.with_crc32c:
            self.requires("crc32c/1.1.2")

    def validate(self):
        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, 11)

    def source(self):
        get(self, url="https://github.com/harish876/docstore/archive/refs/tags/0.0.1.zip", strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["LEVELDB_BUILD_TESTS"] = False
        tc.variables["LEVELDB_BUILD_BENCHMARKS"] = False
        tc.variables["HAVE_SNAPPY"] = self.options.with_snappy
        tc.variables["HAVE_CRC32C"] = self.options.with_crc32c
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "docstore")
        self.cpp_info.set_property("cmake_target_name", "docstore::docstore")
        self.cpp_info.libs = ["docstore"]
        if self.options.shared:
            self.cpp_info.defines.append("DOCSTORE_SHARED_LIBRARY")
        elif self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.append("pthread")
