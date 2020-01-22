from concurrent import futures

import grpc

import raft_pb2
import raft_pb2_grpc

import time
from random import randrange

import os.path
import argparse
import pickle
import concurrent.futures
from collections import Counter
import operator

parser = argparse.ArgumentParser()
parser.add_argument("--conf_dir", default="./config_2.config", type=str)
parser.add_argument("--data_dir", default="./data.pck", type=str)
parser.add_argument("--id", default="1", type=str)
parser.add_argument("--host", default="192.168.43.128", type=str)
parser.add_argument("--port", default=4444, type=str)
args = parser.parse_args()


class LocalfileManger(object):

    def __init__(self, _config, _data_dir):
        self.table = self.readConf(_config) #{'id':{'ip:port':something, 'stub':something}, ...}
        self.dataDir = _data_dir
        self.data = self.loadData()
        self.lastIndex = len(self.data['log']) - 1

        print(self.table)

    def readConf(self, _config):
        conf = open(_config, 'r')
        lines = conf.readlines()

        table = {}
        for line in lines:
            contents = line.split(' ')
            table[contents[0]] = {"ip:port": contents[1].split('\n')[0], 'stub': None}
        conf.close()
        return table

    def loadData(self):
        if os.path.isfile(self.dataDir):
            print("file found. loading at {}".format(self.dataDir))
            with open(self.dataDir, 'rb') as file:
                data = pickle.load(file)
        else:
            print("file not found. creating new file at {}".format(self.dataDir))
            log_data = []
            votedFor = None
            currentTerm = 0
            state = ""
            data = {'log': log_data, 'votedFor': votedFor, 'currentTerm': currentTerm, "state": state}
            """
            with open(self.dataDir, 'wb') as file:
                pickle.dump(data, file)"""

        return data

    def setState(self, _index):
        self.data['state'] = self.getLog(_index).decree
        print('set state to {}'.format(self.data['state']))
        self.dumpData()

    def getState(self):
        return self.data['state']

    def getLog(self, _index):

        if _index == 0:
            return raft_pb2.Entry(term=self.getCurrentTerm(), decree=None, index=0)

        _index -= 1
        log_data = self.data['log']
        if len(log_data) <= _index:
            return raft_pb2.Entry(term=self.getCurrentTerm(), decree=None, index=0)
        else:
            return log_data[_index]

    def appendLog(self, _entry, _index=None):
        log_data = self.data['log']

        try:
            if _index is None:
                log_data.append(_entry)
                self.lastIndex = len(log_data) - 1
                print(log_data)
                self.dumpData()
                return True
            else:
                _index -= 1
                if _index < len(log_data):
                    log_data[_index] = _entry

                    index = int(_index) + 1
                    while index < len(log_data):
                        log_data[int(index)] = None
                        index += 1

                    self.lastIndex = _index
                    print(log_data)
                    self.dumpData()
                    return True
                else:
                    log_data.append(_entry)
                    self.lastIndex = len(log_data) - 1
                    print(log_data)
                    self.dumpData()
                    return True
        except:
            print("Error appending")
            return False

    def setVotedFor(self, _votedFor):
        self.data['votedFor'] = _votedFor
        self.dumpData()
        return self.data['votedFor']

    def setCurrentTerm(self, _currentTerm):
        self.data['currentTerm'] = _currentTerm
        self.dumpData()
        return self.data['currentTerm']

    def getCurrentTerm(self):
        return self.data['currentTerm']

    def getVotedFor(self):
        return self.data['votedFor']

    def getMajorityNum(self):
        return (len(self.table)+1)//2 + 1 if (len(self.table)+1) % 2 != 0 else int((len(self.table)+1)/2)

    def getTable(self):
        return self.table

    def getNextIndexTable(self):
        table = {}
        for key in self.table:
            table[key] = self.getLastEntryIndex() + 1
        return table

    def getMatchIndexTable(self):
        table = {}
        for key in self.table:
            table[key] = 0
        return table

    def getLastEntryIndex(self):
        return self.lastIndex + 1

    def logCheck(self, _index):
        if _index == 0:
            return True
        else:
            _index -= 1
            if len(self.data['log']) > _index:
                return self.data['log'][_index] is not None
            else:
                return False

    def termCheck(self, _index, _term):
        if _index == 0:
            return True
        else:
            _index -= 1
            return self.data['log'][_index].term == _term

    def dumpData(self):
        with open(self.dataDir, 'wb') as file:
            pickle.dump(self.data, file)

class RaftServer(raft_pb2_grpc.RaftServerServicer):

    def __init__(self, _conf, _id, _data_dir):
        self.lfManager = LocalfileManger(_conf, _data_dir)
        self.serviceTable = None

        self.majorityNum = self.lfManager.getMajorityNum()

        self.follower = True
        self.candidate = False
        self.leader = False
        self.lastTimeHearFromLeader = time.time()

        self.myID = int(_id)
        self.leaderID = -1
        self.leaderTerm = -1

        self.nextIndex = self.lfManager.getNextIndexTable()
        self.matchIndex = self.lfManager.getNextIndexTable()

        self.commitIndex = 0
        self.lastApplied = 0


    def joinTheService(self):
        self.serviceTable = self.lfManager.getTable()
        del self.serviceTable[str(self.myID)]
        for key in self.serviceTable:
            while True:
                try:
                    channel = grpc.insecure_channel(self.serviceTable[key]['ip:port'])
                    stub = raft_pb2_grpc.RaftServerStub(channel)
                    self.serviceTable[key]['stub'] = stub
                    print("creating stub to {} successes".format(self.serviceTable[key]['ip:port']))
                    break
                except Exception as e:
                    print(e)
                    print("createing stub to {} fails".format(self.serviceTable[key]['ip:port']))

    def isLeaderAlive(self):
        return (time.time() - self.lastTimeHearFromLeader) < 10 if not self.leader else True

    def leaderCheck(self):
        while True:
            time.sleep(1)
            while not self.isLeaderAlive():
                print("become candidate")
                # leader is dead, I am going to become a candidate.
                self.follower = False
                self.candidate = True

                self.leader = self.gatheringVotes()
                if self.leader:
                    print('become leader with term {}'.format(self.lfManager.getCurrentTerm()))
                    self.follower = False
                    self.candidate = False
                    self.leaderID = self.myID
                    self.nextIndex = self.lfManager.getNextIndexTable()
                    self.matchIndex = self.lfManager.getNextIndexTable()
                    self.commitIndex = self.lfManager.getLastEntryIndex()

                    while self.leader:
                        self.broadcastImAlive()
                        self.commitEntry()
                        time.sleep(1)
                else:
                    pass

            self.follower = True
            self.candidate = False
            self.leader = False

            self.commitEntry()

    def gatheringVotes(self):
        timelimit = randrange(1, 5)
        votesCount = 0
        self.lfManager.setCurrentTerm(self.lfManager.getCurrentTerm() + 1)
        print("my term is {}".format(self.lfManager.getCurrentTerm()))
        self.lfManager.setVotedFor(self.myID)
        votesCount += 1

        start = time.time()
        for key in self.serviceTable:
            stub = self.serviceTable[key]['stub']
            try:
                rsp = self.sendRequestVote(stub)
                print("sending voting request to {}".format(key))
                if rsp.voteGranted:
                    votesCount += 1
                    print('vote increased to {}'.format(votesCount))
                else:
                    print('vote is not granted by {}'.format(key))
            except:
                print('error transmission')
            if time.time() - start > timelimit or self.isLeaderAlive() or votesCount >= self.majorityNum:
                break

        while time.time() - start < timelimit:
            time.sleep(1)

        return votesCount >= self.majorityNum and self.candidate

    def sendRequestVote(self, stub):
        response = stub.RequestVote(raft_pb2.RequestVoteRequest(term=self.lfManager.getCurrentTerm(),
                                                                cadidateId=self.myID,
                                                                lastLogIndex=self.lfManager.getLastEntryIndex(),
                                                                lastLogTerm=self.lfManager.getCurrentTerm()))
        return response

    def broadcastImAlive(self):
        requests = []
        for key in self.serviceTable:
            stub = self.serviceTable[key]['stub']
            try:
                resp = self.sendHeartBeat(stub)
            except:
                pass

    def sendHeartBeat(self, stub):
        response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(leaderCommit=self.commitIndex, term=self.lfManager.getCurrentTerm(), leaderId=int(self.myID), entry=None))
        return response

    def broadcastReplicate(self):
        count = 1
        for key in self.serviceTable:
            rsp = None
            try:
                rsp = self.sendAppendRequest(key)
            except:
                pass

            if rsp:
                count += 1
                print('append success')
            else:
                print(rsp)

        if count >= self.majorityNum:
            print('commitIndex + 1')
            self.commitIndex += 1
            if count > 1:
                self.commitCheck()
            else:
                pass
        else:
            print('broadcast fails')
            print('count:{} majority:{}'.format(count, self.majorityNum))
        return self.commitIndex

    def sendAppendRequest(self, key):
        stub = self.serviceTable[key]['stub']

        while self.lfManager.getLastEntryIndex() >= self.nextIndex[key]:
            entry = self.lfManager.getLog(self.nextIndex[key])
            try:
                req = raft_pb2.AppendEntriesRequest(term=self.lfManager.getCurrentTerm(),
                                                    leaderId=self.leaderID,
                                                    prevLogIndex=max(self.nextIndex[key]-1, 0),
                                                    prevLogTerm=self.lfManager.getLog(self.nextIndex[key]-1).term,
                                                    entry=entry,
                                                    leaderCommit=self.commitIndex)
                print(req.prevLogIndex)
                rsp = stub.AppendEntries(req)
                if rsp.success:
                    print('append success with {}'.format(self.nextIndex[key]))
                    self.matchIndex[key] = self.nextIndex[key]
                    self.nextIndex[key] += 1
                else:
                    print('append fails decremented {}'.format(self.nextIndex[key]))
                    self.nextIndex[key] -= 1
            except IndexError as iex:
                print(iex.args)
                print("Error broadcasting to {}".format(key))
                return False

        return True

    def commitCheck(self):
        matchList = [self.matchIndex[key] for key in self.matchIndex]
        counted = dict(Counter(matchList)) # {'key(index):count}
        N = max(counted.items(), key=operator.itemgetter(1))[0] # get the key(index) of largest count
        if N > self.commitIndex:
            if counted[N] >= self.majorityNum:
                log_term = self.lfManager.getLog(N).term
                if log_term == self.lfManager.getCurrentTerm():
                    self.commitIndex = N
                else:
                    pass
            else:
                pass
        else:
            pass

    def commitEntry(self):
        while self.lastApplied < self.commitIndex:
            print("lastApplied:{} Commit:{}".format(self.lastApplied, self.commitIndex))
            self.lastApplied += 1
            self.lfManager.setState(self.lastApplied)

    def AppendEntries(self, append_req, context):

        if append_req.term < self.lfManager.getCurrentTerm():
            return raft_pb2.AppendEntriesResponse(term=self.lfManager.getCurrentTerm(), success=0)
        else:
            print('received heartbeat request from {} with term {}'.format(append_req.leaderId, append_req.term))
            self.lastTimeHearFromLeader = time.time()
            if append_req.entry.decree == "" and append_req.entry.term == 0 and append_req.entry.index == 0:
                self.commitIndex = append_req.leaderCommit
                if self.candidate or self.leader or self.leaderID == -1:
                    self.lfManager.setCurrentTerm(append_req.term)

                    self.leaderID = append_req.leaderId

                    self.leader = False
                    self.candidate = False
                    self.follower = True

                    print('Leader is {} and the term is updated to {}'.format(self.leaderID, self.lfManager.getCurrentTerm()))
                else:
                    pass
                return raft_pb2.AppendEntriesResponse(term=self.lfManager.getCurrentTerm(), success=1)
            else:
                prevLogIndex = append_req.prevLogIndex
                prevLogTerm = append_req.prevLogTerm

                if self.lfManager.logCheck(prevLogIndex) and self.lfManager.termCheck(prevLogIndex, prevLogTerm):
                    leaderCommit = append_req.leaderCommit
                    entry = append_req.entry

                    print("receive new entry-index:{} term:{} decree:{}".format(entry.index, entry.term, entry.decree))
                    self.lastNewEntry = entry.index
                    self.lfManager.appendLog(entry, _index=self.lastNewEntry)

                    print("leaderCommit:{} lastNewEntry:{}".format(leaderCommit, self.lastNewEntry))
                    if append_req.leaderCommit > self.commitIndex:
                        self.commitIndex = min(leaderCommit, self.lastNewEntry)
                    return raft_pb2.AppendEntriesResponse(term=self.lfManager.getCurrentTerm(), success=1)
                else:
                    return raft_pb2.AppendEntriesResponse(term=self.lfManager.getCurrentTerm(), success=0)

    def RequestVote(self, vote_req, context):
        print("get request vote from {} with term {}".format(vote_req.cadidateId, vote_req.term))
        reqTerm = vote_req.term
        if reqTerm <= self.lfManager.getCurrentTerm():
            print("Vote for False first condition")
            return raft_pb2.RequestVoteResponse(term=self.lfManager.getCurrentTerm(), voteGranted=False)
        elif self.lfManager.getVotedFor() is None:
            print("Vote for True second condition")
            self.lfManager.setVotedFor(vote_req.cadidateId)
            self.lfManager.setCurrentTerm(vote_req.term)
            return raft_pb2.RequestVoteResponse(term=self.lfManager.getCurrentTerm(), voteGranted=True)
        elif reqTerm > self.lfManager.getCurrentTerm():
            print("Vote for True third condition")
            self.lfManager.setVotedFor(vote_req.cadidateId)
            self.lfManager.setCurrentTerm(vote_req.term)
            return raft_pb2.RequestVoteResponse(term=self.lfManager.getCurrentTerm(), voteGranted=True)
        elif self.lfManager.logCheck(vote_req.lastLogIndex) and self.lfManager.termCheck(vote_req.lastLogIndex, vote_req.lastLogTerm):
            print("Vote for Fourth condition")
            self.lfManager.setVotedFor(vote_req.cadidateId)
            self.lfManager.setCurrentTerm(vote_req.term)
            return raft_pb2.RequestVoteResponse(term=self.lfManager.getCurrentTerm(), voteGranted=True)
        else:
            print("Vote for False fourth condition")
            return raft_pb2.RequestVoteResponse(term=self.lfManager.getCurrentTerm(), voteGranted=False)

    def ClientAppend(self, client_req, context):
        if self.leader:
            index = 0
            try:
                decree = client_req.decree
                newEntry = raft_pb2.Entry(index=self.lfManager.getLastEntryIndex() + 1, term=self.lfManager.getCurrentTerm(), decree=decree)
                self.lfManager.appendLog(newEntry)
                print("Entry Success")
                index = self.broadcastReplicate()
                print("broadcast success")
            except:
                print("error appending")
            return raft_pb2.ClientAppendResponse(index=index, rc=0, leader=self.leaderID)
        else:
            return raft_pb2.ClientAppendResponse(index=0, rc=1, leader=self.leaderID)

    def ClientRequestIndex(self, client_req, context):

        if self.leader:
            index = client_req.index

            value = self.lfManager.getLog(index)
            if value.index == 0:
                index = self.commitIndex
                value = self.lfManager.getLog(index)

            return raft_pb2.ClientRequestIndexResponse(index=index, leader=self.leaderID, decree=value.decree, rc=0)
        else:
            return raft_pb2.ClientAppendResponse(index=0, rc=1, leader=self.leaderID)


if __name__ == "__main__":
    print("Listening to {} at {}".format(args.port, args.host))
    raft = RaftServer(args.conf_dir, args.host.split(".")[3], args.data_dir)
    #raft = RaftServer(args.conf_dir, args.id, args.data_dir)

    server = grpc.server(futures.ThreadPoolExecutor())
    raft_pb2_grpc.add_RaftServerServicer_to_server(raft, server)
    server.add_insecure_port("[::]:{}".format(args.port))
    server.start()
    raft.joinTheService()
    raft.leaderCheck()

    while True:
        time.sleep(1)