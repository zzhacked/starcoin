// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use libp2p::futures::channel::oneshot;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

pub mod network_state;

pub use libp2p::core::{identity, multiaddr, Multiaddr, PeerId, PublicKey};
pub use libp2p::request_response::{InboundFailure, OutboundFailure};
pub use libp2p::{build_multiaddr, multihash};
pub use sc_peerset::ReputationChange;

/// Parses a string address and splits it into Multiaddress and PeerId, if
/// valid.
///
/// # Example
///
/// ```
/// # use network_p2p_types::{Multiaddr, PeerId, parse_str_addr};
/// let (peer_id, addr) = parse_str_addr(
///    "/ip4/198.51.100.19/tcp/30333/p2p/QmSk5HQbn6LhUwDiNMseVUjuRYhEtYj4aUZ6WfWoGURpdV"
/// ).unwrap();
/// assert_eq!(peer_id, "QmSk5HQbn6LhUwDiNMseVUjuRYhEtYj4aUZ6WfWoGURpdV".parse::<PeerId>().unwrap());
/// assert_eq!(addr, "/ip4/198.51.100.19/tcp/30333".parse::<Multiaddr>().unwrap());
/// ```
///
pub fn parse_str_addr(addr_str: &str) -> Result<(PeerId, Multiaddr), ParseErr> {
    let addr: Multiaddr = addr_str.parse()?;
    parse_addr(addr)
}

/// Splits a Multiaddress into a Multiaddress and PeerId.
pub fn parse_addr(mut addr: Multiaddr) -> Result<(PeerId, Multiaddr), ParseErr> {
    let who = match addr.pop() {
        Some(multiaddr::Protocol::P2p(key)) => {
            PeerId::from_multihash(key).map_err(|_| ParseErr::InvalidPeerId)?
        }
        _ => return Err(ParseErr::PeerIdMissing),
    };

    Ok((who, addr))
}

/// Build memory protocol Multiaddr by port
pub fn memory_addr(port: u64) -> Multiaddr {
    build_multiaddr!(Memory(port))
}

/// Generate a random memory protocol Multiaddr
pub fn random_memory_addr() -> Multiaddr {
    memory_addr(rand::random::<u64>())
}

/// Check the address is a memory protocol Multiaddr.
pub fn is_memory_addr(addr: &Multiaddr) -> bool {
    addr.iter()
        .any(|protocol| matches!(protocol, libp2p::core::multiaddr::Protocol::Memory(_)))
}

/// Address of a node, including its identity.
///
/// This struct represents a decoded version of a multiaddress that ends with `/p2p/<peerid>`.
///
/// # Example
///
/// ```
/// # use network_p2p_types::{Multiaddr, PeerId, MultiaddrWithPeerId};
/// let addr: MultiaddrWithPeerId =
///     "/ip4/198.51.100.19/tcp/30333/p2p/QmSk5HQbn6LhUwDiNMseVUjuRYhEtYj4aUZ6WfWoGURpdV".parse().unwrap();
/// assert_eq!(addr.peer_id.to_base58(), "QmSk5HQbn6LhUwDiNMseVUjuRYhEtYj4aUZ6WfWoGURpdV");
/// assert_eq!(addr.multiaddr.to_string(), "/ip4/198.51.100.19/tcp/30333");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MultiaddrWithPeerId {
    /// Address of the node.
    pub multiaddr: Multiaddr,
    /// Its identity.
    pub peer_id: PeerId,
}

impl MultiaddrWithPeerId {
    pub fn new(multiaddr: Multiaddr, peer_id: PeerId) -> Self {
        Self { multiaddr, peer_id }
    }

    /// Concatenates the multiaddress and peer ID into one multiaddress containing both.
    pub fn concat(&self) -> Multiaddr {
        let proto = multiaddr::Protocol::P2p(From::from(self.peer_id.clone()));
        self.multiaddr.clone().with(proto)
    }
}

impl fmt::Display for MultiaddrWithPeerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.concat(), f)
    }
}

impl FromStr for MultiaddrWithPeerId {
    type Err = ParseErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (peer_id, multiaddr) = parse_str_addr(s)?;
        Ok(MultiaddrWithPeerId { peer_id, multiaddr })
    }
}

impl From<MultiaddrWithPeerId> for String {
    fn from(ma: MultiaddrWithPeerId) -> String {
        format!("{}", ma)
    }
}

impl TryFrom<String> for MultiaddrWithPeerId {
    type Error = ParseErr;
    fn try_from(string: String) -> Result<Self, Self::Error> {
        string.parse()
    }
}

impl Into<Multiaddr> for MultiaddrWithPeerId {
    fn into(self) -> Multiaddr {
        self.concat()
    }
}

/// Error that can be generated by `parse_str_addr`.
#[derive(Debug)]
pub enum ParseErr {
    /// Error while parsing the multiaddress.
    MultiaddrParse(multiaddr::Error),
    /// Multihash of the peer ID is invalid.
    InvalidPeerId,
    /// The peer ID is missing from the address.
    PeerIdMissing,
}

impl fmt::Display for ParseErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseErr::MultiaddrParse(err) => write!(f, "{}", err),
            ParseErr::InvalidPeerId => write!(f, "Peer id at the end of the address is invalid"),
            ParseErr::PeerIdMissing => write!(f, "Peer id is missing from the address"),
        }
    }
}

impl std::error::Error for ParseErr {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParseErr::MultiaddrParse(err) => Some(err),
            ParseErr::InvalidPeerId => None,
            ParseErr::PeerIdMissing => None,
        }
    }
}

impl From<multiaddr::Error> for ParseErr {
    fn from(err: multiaddr::Error) -> ParseErr {
        ParseErr::MultiaddrParse(err)
    }
}

/// Error in a request.
#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum RequestFailure {
    /// Remote has closed the substream before answering, thereby signaling that it considers the
    /// request as valid, but refused to answer it.
    Refused,
    /// Problem on the network.
    #[display(fmt = "Problem on the network")]
    Network(#[error(ignore)] OutboundFailure),
}

/// Error when processing a request sent by a remote.
#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum ResponseFailure {
    /// Internal response builder is too busy to process this request.
    Busy,
    /// Problem on the network.
    #[display(fmt = "Problem on the network")]
    Network(#[error(ignore)] InboundFailure),
}

/// A single request received by a peer on a request-response protocol.
#[derive(Debug)]
pub struct IncomingRequest {
    /// Who sent the request.
    pub peer: PeerId,

    /// Request sent by the remote. Will always be smaller than
    /// [`ProtocolConfig::max_request_size`].
    pub payload: Vec<u8>,

    /// Channel to send back the response to.
    pub pending_response: oneshot::Sender<Vec<u8>>,
}

#[derive(Debug)]
pub struct ProtocolRequest {
    pub protocol: Cow<'static, str>,
    pub request: IncomingRequest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_address() {
        let addr = random_memory_addr();
        assert!(is_memory_addr(&addr));
    }
}
