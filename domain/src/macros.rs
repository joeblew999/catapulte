#[macro_export]
macro_rules! genid {
    ($struct_name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $struct_name(uuid::Uuid);

        impl Default for $struct_name {
            fn default() -> Self {
                // `now_v7` reads `SystemTime`, which panics on wasm32
                // ("time not implemented"). On the Workers runtime fall back to
                // a random v4 id — ordering relies on `created_at_ms`, not the id.
                #[cfg(not(target_arch = "wasm32"))]
                {
                    Self(uuid::Uuid::now_v7())
                }
                #[cfg(target_arch = "wasm32")]
                {
                    Self(uuid::Uuid::new_v4())
                }
            }
        }

        impl From<uuid::Uuid> for $struct_name {
            fn from(value: uuid::Uuid) -> Self {
                Self(value)
            }
        }

        impl $struct_name {
            pub const fn as_uuid(&self) -> uuid::Uuid {
                self.0
            }
        }
    };
}
