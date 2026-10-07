// Equality-only .NET 8 ICU profile: Unicode 17 BMP and Unicode 15 supplementary.
const NO_CASING_PAGES: [u8; 32] = [
    0x00, 0x00, 0x4c, 0x00, 0x37, 0xe0, 0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xfe, 0xf4, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xc8,
];

pub(super) struct KeyCasing {
    bmp: icu_casemap::CaseMapperBorrowed<'static>,
    supplementary: icu_casemap_net8::CaseMapper,
}

impl Default for KeyCasing {
    fn default() -> Self {
        Self {
            bmp: icu_casemap::CaseMapper::new(),
            supplementary: icu_casemap_net8::CaseMapper::new(),
        }
    }
}

impl KeyCasing {
    pub(super) fn key(&self, value: &str) -> String {
        value.chars().map(|value| self.uppercase(value)).collect()
    }

    pub(super) fn uppercase(&self, value: char) -> char {
        match value {
            'a'..='z' | '\u{e0}'..='\u{f6}' | '\u{f8}'..='\u{fe}' => char::from(value as u8 - 0x20),
            '\u{b5}' => '\u{39c}',
            '\u{ff}' => '\u{178}',
            _ if value <= '\u{ff}' => value,
            '\u{131}' | '\u{17f}' => value,
            _ if value > '\u{ffff}' => self.supplementary.simple_uppercase(value),
            _ => {
                let page = value as usize >> 8;
                // The static .NET bitmap is MSB-first, unlike an ordinary bitset.
                if NO_CASING_PAGES[page >> 3] & (0x80 >> (page & 7)) != 0 {
                    value
                } else {
                    self.bmp.simple_uppercase(value)
                }
            }
        }
    }
}
