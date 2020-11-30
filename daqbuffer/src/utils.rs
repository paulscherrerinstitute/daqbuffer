pub async fn delay_us(mu: u64) {
  tokio::time::delay_for(std::time::Duration::from_micros(mu)).await;
}

pub async fn delay_io_short() {
  delay_us(400).await;
}

pub async fn delay_io_medium() {
  delay_us(1200).await;
}
