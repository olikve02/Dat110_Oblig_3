/**
 *
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {

	private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
	/** lock variables */
	private boolean CS_BUSY = false;						// indicate to be in critical section (accessing a shared resource)
	private boolean WANTS_TO_ENTER_CS = false;				// indicate to want to enter CS
	private List<Message> queueack; 						// queue for acknowledged messages
	private List<Message> mutexqueue;						// queue for storing process that are denied permission. We really don't need this for quorum-protocol

	private LamportClock clock;								// lamport clock
	private Node node;
	List<Message> mutexQueue; // Define mutexQueue as a List<Message>


	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;

		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}

	public synchronized void acquireLock() {
		CS_BUSY = true;
	}

	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {

		logger.info(node.nodename + " wants to access CS");
		// clear the queueack before requesting for votes

		// clear the mutexqueue

		// increment clock

		// adjust the clock on the message, by calling the setClock on the message

		// wants to access resource - set the appropriate lock variable


		// start MutualExclusion algorithm

		// first, call removeDuplicatePeersBeforeVoting. A peer can hold/contain 2 replicas of a file. This peer will appear twice

		// multicast the message to activenodes (hint: use multicastMessage)

		// check that all replicas have replied (permission)

		// if yes, acquireLock

		// node.broadcastUpdatetoPeers

		// clear the mutexqueue

		// return permission
		queueack.clear();
		mutexqueue.clear();
		clock.increment();
		clock.adjustClock(message.getClock());
		WANTS_TO_ENTER_CS = true;
		List<Message> list = removeDuplicatePeersBeforeVoting();
		multicastMessage(message, removeDuplicatePeersBeforeVoting());
		if (areAllMessagesReturned(list.size())) {
			acquireLock();
			node.broadcastUpdatetoPeers(updates);
			mutexqueue.clear();
			return true;
		}
		return false;
	}

	// multicast message to other processes including self
	public void multicastMessage(Message message, List<Message> activeNodes) {
		logger.info("Number of peers to vote = " + activeNodes.size());

		for (Message m : activeNodes) {
			try {
				NodeInterface pStub = Util.getProcessStub(m.getNodeName(), m.getPort());
				// call onMutexRequestReceived()
				pStub.onMutexRequestReceived(message);
			} catch (RemoteException e) {
				logger.error("Error communicating with node: " + m.getNodeName(), e);
			}
		}
	}

	public void onMutexRequestReceived(Message message) throws RemoteException {

		// Increment the local clock (assuming clock is defined somewhere)
		clock.increment();
		int caseId = -1;

		if (message.getNodeName().equals(node.nodename) && message.getPort() == node.getPort()) {
			message.setAcknowledged(true);
			onMutexAcknowledgementReceived(message);
		} else {
			if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
				caseId = 0;
			} else if (CS_BUSY) {
				caseId = 1;
			} else {
				caseId = 2;
			}
		}

		// Write if statement to transition to the correct caseId
		switch (caseId) {
			case 0: // Receiver is not accessing shared resource and does not want to (send OK to sender)
				NodeInterface senderStub = Util.getProcessStub(message.getNodeName(), message.getPort());
				senderStub.onMutexRequestReceived(message);
				break;
			case 1: // Receiver already has access to the resource (don't reply but queue the request)
				mutexQueue.add(message); // Ensure mutexQueue is initialized before adding elements
				break;
			case 2: // Receiver wants to access resource but is yet to - compare own message clock to received message's clock
				if (clock.getClock() < message.getClock() ||
						(clock.getClock() == message.getClock() && node.nodename.compareTo(message.getNodeName()) < 0)) {
					mutexQueue.add(message); // Ensure mutexQueue is initialized before adding elements
				} else {
					NodeInterface senderStub = Util.getProcessStub(message.getNodeName(), message.getPort());
					senderStub.onMutexAcknowledgementReceived(new Message(node.nodename, node.port));
				}
				break;
			default:
				// Handle the default case if needed
				break;
		}

		// Check for decision (assuming doDecisionAlgorithm is defined somewhere)
		doDecisionAlgorithm(message, mutexQueue, caseId);
	}


	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
		String procName = message.getNodeName();
		int port = message.getPort();

		switch (condition) {
			/** case 1: Receiver is not accessing shared resource and does not want to (send OK to sender) */
			case 0: {
				// Get a stub for the sender from the registry
				NodeInterface pStub = Util.getProcessStub(procName, port);
				// Acknowledge message
				message.setAcknowledged(true);
				// Send acknowledgment back by calling onMutexAcknowledgementReceived()
				pStub.onMutexAcknowledgementReceived(message);
				break;
			}
			/** case 2: Receiver already has access to the resource (don't reply but queue the request) */
			case 1: {
				// Queue this message
				queue.add(message);
				break;
			}
			/**
			 *  case 3: Receiver wants to access resource but is yet to (compare own message clock to received message's clock
			 *  the message with lower timestamp wins) - send OK if received is lower. Queue message if received is higher
			 */
			case 2: {
				// Check the clock of the sending process (note that the correct clock is in the message)
				int sClock = message.getClock();
				// Own clock for the multicast message (note that the correct clock is in the message)
				int oClock = clock.getClock();

				// Compare clocks, the lowest wins
				if (sClock < oClock || (sClock == oClock && message.getNodeName().compareTo(node.getNodeName()) < 0)) {
					// If sender wins, acknowledge the message, obtain a stub and call onMutexAcknowledgementReceived()
					NodeInterface pStub = Util.getProcessStub(procName, port);
					message.setAcknowledged(true);
					pStub.onMutexAcknowledgementReceived(message);
				} else {
					// If sender loses, queue it
					queue.add(message);
				}
				break;
			}
			default:
				break;
		}
	}

	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {

		// add message to queueack
		queueack.add(message);

	}

	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) {
		logger.info("Releasing locks from = "+activenodes.size());

		// iterate over the activenodes

		// obtain a stub for each node from the registry

		// call releaseLocks()

		for (Message m: activenodes) {
			NodeInterface pStub = Util.getProcessStub(m.getNodeName(), m.getPort());
			try {
				pStub.releaseLocks();
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}

	private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
		logger.info(node.getNodeName()+": size of queueack = "+queueack.size());

		// check if the size of the queueack is same as the numvoters

		// clear the queueack

		// return true if yes and false if no
		if (queueack.size() == numvoters) {
			queueack.clear();
			return true;
		}

		return false;
	}

	private List<Message> removeDuplicatePeersBeforeVoting() {

		List<Message> uniquepeer = new ArrayList<Message>();
		for(Message p : node.activenodesforfile) {
			boolean found = false;
			for(Message p1 : uniquepeer) {
				if(p.getNodeName().equals(p1.getNodeName())) {
					found = true;
					break;
				}
			}
			if(!found)
				uniquepeer.add(p);
		}
		return uniquepeer;
	}
}
