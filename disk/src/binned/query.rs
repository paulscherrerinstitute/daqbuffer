use err::Error;
use http::request::Parts;
use netpod::query::{agg_kind_from_binning_scheme, binning_scheme_append_to_url, CacheUsage};
use netpod::{channel_from_pairs, AggKind, AppendToUrl, ByteSize, Channel, PreBinnedPatchCoord};
use std::collections::BTreeMap;
use url::Url;

#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
    cache_usage: CacheUsage,
    disk_io_buffer_size: usize,
    disk_stats_every: ByteSize,
    report_error: bool,
}

impl PreBinnedQuery {
    pub fn new(
        patch: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        cache_usage: CacheUsage,
        disk_io_buffer_size: usize,
        disk_stats_every: ByteSize,
        report_error: bool,
    ) -> Self {
        Self {
            patch,
            agg_kind,
            channel,
            cache_usage,
            disk_io_buffer_size,
            disk_stats_every,
            report_error,
        }
    }

    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let mut pairs = BTreeMap::new();
        for (j, k) in url.query_pairs() {
            pairs.insert(j.to_string(), k.to_string());
        }
        let pairs = pairs;
        let bin_t_len = pairs
            .get("binTlen")
            .ok_or(Error::with_msg("missing binTlen"))?
            .parse()?;
        let patch_t_len = pairs
            .get("patchTlen")
            .ok_or(Error::with_msg("missing patchTlen"))?
            .parse()?;
        let patch_ix = pairs
            .get("patchIx")
            .ok_or(Error::with_msg("missing patchIx"))?
            .parse()?;
        let disk_stats_every = pairs
            .get("diskStatsEveryKb")
            .ok_or(Error::with_msg("missing diskStatsEveryKb"))?;
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse diskStatsEveryKb {:?}", e)))?;
        let ret = Self {
            patch: PreBinnedPatchCoord::new(bin_t_len, patch_t_len, patch_ix),
            channel: channel_from_pairs(&pairs)?,
            agg_kind: agg_kind_from_binning_scheme(&pairs).unwrap_or(AggKind::DimXBins1),
            cache_usage: CacheUsage::from_pairs(&pairs)?,
            disk_io_buffer_size: pairs
                .get("diskIoBufferSize")
                .map_or("4096", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse diskIoBufferSize {:?}", e)))?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
            report_error: pairs
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
        };
        Ok(ret)
    }

    pub fn from_request(head: &Parts) -> Result<Self, Error> {
        let s1 = format!("dummy:{}", head.uri);
        let url = Url::parse(&s1)?;
        Self::from_url(&url)
    }

    pub fn patch(&self) -> &PreBinnedPatchCoord {
        &self.patch
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn agg_kind(&self) -> &AggKind {
        &self.agg_kind
    }

    pub fn disk_stats_every(&self) -> ByteSize {
        self.disk_stats_every.clone()
    }

    pub fn cache_usage(&self) -> CacheUsage {
        self.cache_usage.clone()
    }

    pub fn disk_io_buffer_size(&self) -> usize {
        self.disk_io_buffer_size
    }
}

impl AppendToUrl for PreBinnedQuery {
    fn append_to_url(&self, url: &mut Url) {
        self.patch.append_to_url(url);
        binning_scheme_append_to_url(&self.agg_kind, url);
        let mut g = url.query_pairs_mut();
        g.append_pair("channelBackend", &self.channel.backend);
        g.append_pair("channelName", &self.channel.name);
        g.append_pair("cacheUsage", &format!("{}", self.cache_usage.query_param_value()));
        g.append_pair("diskIoBufferSize", &format!("{}", self.disk_io_buffer_size));
        g.append_pair("diskStatsEveryKb", &format!("{}", self.disk_stats_every.bytes() / 1024));
        g.append_pair("reportError", &format!("{}", self.report_error()));
    }
}
