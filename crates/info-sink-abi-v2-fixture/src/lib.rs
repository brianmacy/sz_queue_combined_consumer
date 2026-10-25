//! Test fixture: a plugin built against a hypothetical INFO sink ABI version
//! 2 whose other entry points were renamed. It exports ONLY
//! `sz_is_abi_version`, so a loader that checks the version first reports a
//! version mismatch instead of a confusing missing-symbol error.

/// The ABI version this fixture claims.
pub const FIXTURE_ABI_VERSION: u32 = 2;

/// `uint32_t sz_is_abi_version(void)`.
#[unsafe(no_mangle)]
pub extern "C" fn sz_is_abi_version() -> u32 {
    FIXTURE_ABI_VERSION
}
