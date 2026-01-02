use rand::Rng;

pub fn generate_random_bytes(length: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut ret = vec![0u8; length];
    rng.fill(&mut ret[..]);

    ret
}