package udpfsd

import (
	"errors"
	"log"
	"net"

	"github.com/pcm720/udpfsd/udprdma"
)

func (s *Server) discoveryHandler() {
	defer s.wg.Done()

	buf := make([]byte, 2048)
	s.discConn.SetReadBuffer(2048)
	for {
		n, addr, err := s.discConn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Printf("udpfsd/discovery: connection has been closed")
				return
			}
			if s.verbose {
				log.Printf("udpfsd/discovery: read error: %v", err)
			}
			continue
		}
		if n < 6 {
			// UDPRDMA discovery packet must be at least 6 bytes long
			continue
		}

		reply, err := udprdma.ProcessDiscoveryPacket(buf[:n], udprdma.Service_UDPFS)
		if err != nil {
			log.Printf("udpfsd/discovery: [%s]: %v", addr, err)
			continue
		}
		// A known peer discovering the server again is about to start a new session
		s.Lock()
		if p, ok := s.cMap[addr.AddrPort()]; ok && p.Connection != nil {
			p.GetUDPRDMASession().PeerDiscovered()
		}
		s.Unlock()
		s.dataConn.WriteToUDP(reply, addr)
		log.Printf("[%s]: discovery request received", addr)
	}
}
