use std::io::{Read, Write};

pub struct EzReader<T: Read> {
    inner: T,
}

impl <T: Read> EzReader<T> {
    pub fn new(reader: T) -> Self {
        EzReader {
            inner: reader,
        }
    }

    pub fn read_u8(&mut self) -> anyhow::Result<u8> {
        let mut buf = [0u8;1];
        self.inner.read_exact(&mut buf)?;

        Ok(buf[0])
    }

    pub fn read_bool(&mut self) -> anyhow::Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    pub fn read_u16(&mut self) -> anyhow::Result<u16> {
        let mut buf = [0u8;2];
        self.inner.read_exact(&mut buf)?;

        Ok(u16::from_be_bytes(buf))
    }

    pub fn read_u32(&mut self) -> anyhow::Result<u32> {
        let mut buf = [0u8;4];
        self.inner.read_exact(&mut buf)?;

        Ok(u32::from_be_bytes(buf))
    }

    pub fn read_u64(&mut self) -> anyhow::Result<u64> {
        let mut buf = [0u8;8];
        self.inner.read_exact(&mut buf)?;

        Ok(u64::from_be_bytes(buf))
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> anyhow::Result<()> {
        self.inner.read_exact(buf)?;
        Ok(())
    }

    pub fn read_to_end(&mut self, buf: &mut Vec<u8>) -> anyhow::Result<()> {
        self.inner.read_to_end(buf)?;
        Ok(())
    }
}

pub struct EzWriteBuf {
    buf: Vec<u8>
}

impl Default for EzWriteBuf {
    fn default() -> Self {
        EzWriteBuf {
            buf: Vec::new(),
        }
    }
}

impl EzWriteBuf {
    pub fn write_u8(&mut self, value: u8) -> anyhow::Result<()> {
        self.buf.push(value);
        Ok(())
    }

    pub fn write_bool(&mut self, value: bool) -> anyhow::Result<()> {
        self.write_u8(if value { 1 } else { 0 })?;
        Ok(())
    }

    pub fn write_u16(&mut self, value: u16) -> anyhow::Result<()> {
        self.buf.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    pub fn write_u32(&mut self, value: u32) -> anyhow::Result<()> {
        self.buf.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    pub fn write_u64(&mut self, value: u64) -> anyhow::Result<()> {
        self.buf.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    pub fn write_all_slice(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        self.buf.extend_from_slice(buf);
        Ok(())
    }

    pub fn write_all(&mut self, buf: &Vec<u8>) -> anyhow::Result<()> {
        self.buf.extend(buf);
        Ok(())
    }

    pub fn consume_bytes(self) -> Vec<u8> {
        self.buf
    }
}