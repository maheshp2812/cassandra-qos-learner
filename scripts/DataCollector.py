from javax.management.remote import JMXConnector
from javax.management.remote import JMXConnectorFactory
from javax.management.remote import JMXServiceURL
from javax.management import MBeanServerConnection
from javax.management import MBeanInfo
from javax.management import ObjectName
from java.lang import String
 
from jarray import array
import sys  
import csv 
import os
import signal
import shutil
from subprocess import Popen, PIPE

count={'count':0}
ReadPrev={'readprev':-1}
WritePrev={'writeprev':-1}
output=[]
output.append(['AntiEntropyStageActive','CacheCleanupExecutorActive','CompactionExecutorActive','GossipStageActive','HintsDispatcherActive','InternalResponseStageActive','MemtableFlushWriterActive','MemtablePostFlushActive','MemtableReclaimMemoryActive','MigrationStageActive','MiscStageActive','PendingRangeCalculatorActive','PerDiskMemtableFlushWriter_0Active','SamplerActive','SecondaryIndexManagementActive','ValidationExecutorActive','CounterMutationStageActive','MutationStageActive','ReadRepairStageActive','ReadStageActive','RequestResponseStageActive','ViewMutationStageActive','Native_Transport_RequestsActive','Cores','CPU_Utilization','Memory_Utilization','IO_Read_Bytes','IO_Write_Bytes','Read Latency','Write Latency'])
def cpu_mem_usage(pid):
	p=Popen("top -p "+str(pid)+" -b -n 1 |grep "+str(pid)+" | awk '{print $9,$10}'", shell=True,stdout=PIPE, stderr=PIPE)
	out, err = p.communicate()
	cpu=str(out.split()[0])
        mem=str(out.split()[1])
	return (cpu,mem)
def io_usage(pid):
	p=Popen("cat /proc/"+str(pid)+"/io | awk 'BEGIN{r=0;w=0} /^read_bytes*/ {r= $2};/^write_bytes*/ {w= $2} END{ print r,w}'", shell=True,stdout=PIPE, stderr=PIPE)
	out, err = p.communicate()
	read_bytes=str(out.split()[0])
	write_bytes=str(out.split()[1])
	return (read_bytes,write_bytes)
def AntiEntropyStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=AntiEntropyStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def CacheCleanupExecutorActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=CacheCleanupExecutor,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def CompactionExecutorActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=CompactionExecutor,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def GossipStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=GossipStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def HintsDispatcherActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=HintsDispatcher,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def InternalResponseStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=InternalResponseStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def MemtableFlushWriterActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=MemtableFlushWriter,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def MemtablePostFlushActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=MemtablePostFlush,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def MemtableReclaimMemoryActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=MemtableReclaimMemory,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def MigrationStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=MigrationStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def MiscStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=MiscStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def PendingRangeCalculatorActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=PendingRangeCalculator,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def PerDiskMemtableFlushWriter_0Active():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=PerDiskMemtableFlushWriter_0,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def SamplerActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=Sampler,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def SecondaryIndexManagementActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=SecondaryIndexManagement,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def ValidationExecutorActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=ValidationExecutor,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def CounterMutationStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=CounterMutationStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def MutationStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=MutationStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def ReadRepairStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadRepairStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def ReadStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def RequestResponseStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=RequestResponseStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def ViewMutationStageActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ViewMutationStage,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))

def Native_Transport_RequestsActive():
	objectName = ObjectName("org.apache.cassandra.metrics:type=ThreadPools,path=transport,scope=Native-Transport-Requests,name=ActiveTasks");
	return(mBeanServerConnection.getAttribute(objectName,"Value"))
def ReadLatency():
	objectName = ObjectName("org.apache.cassandra.metrics:type=Keyspace,keyspace=ycsb,name=ReadLatency");
	return(mBeanServerConnection.getAttribute(objectName,"Count"))
def ReadTotalLatency():
	objectName = ObjectName("org.apache.cassandra.metrics:type=Keyspace,keyspace=ycsb,name=ReadTotalLatency");
	return(mBeanServerConnection.getAttribute(objectName,"Count"))
def WriteLatency():
	objectName = ObjectName("org.apache.cassandra.metrics:type=Keyspace,keyspace=ycsb,name=WriteLatency");
	return(mBeanServerConnection.getAttribute(objectName,"Count"))
def WriteTotalLatency():
	objectName = ObjectName("org.apache.cassandra.metrics:type=Keyspace,keyspace=ycsb,name=WriteTotalLatency");
	return(mBeanServerConnection.getAttribute(objectName,"Count"))

def sig_handler(signal, frame):
	fpath=os.path.join(opDir,filename)
	with open(fpath, 'w') as f:
		writer = csv.writer(f)
		writer.writerows(output)
	exit()

if __name__=='__main__':
	signal.signal(signal.SIGINT, sig_handler)
        credentials = array(["",""],String)
        environment = {JMXConnector.CREDENTIALS:credentials}
	if len(sys.argv[1:])==5:
		serverUrl = sys.argv[1]
		outputdir=sys.argv[2]
		cores=sys.argv[3]
		pid=sys.argv[4]
		filename=sys.argv[5]
		opDir=os.path.join(os.path.dirname(__file__),outputdir)
		# if os.path.exists(opDir):
		#	shutil.rmtree(opDir)
		# os.makedirs(opDir)
		jmxServiceUrl = JMXServiceURL('service:jmx:rmi:///jndi/rmi://'+str(serverUrl)+':7199/jmxrmi');
		jmxConnector = JMXConnectorFactory.connect(jmxServiceUrl,environment);
		mBeanServerConnection = jmxConnector.getMBeanServerConnection()
		print('started')
		while True:
			count['count']=count['count']+1
			print(count['count'])
			thread1=AntiEntropyStageActive()
			thread2=CacheCleanupExecutorActive()
			thread3=CompactionExecutorActive()
			thread4=GossipStageActive()
			thread5=HintsDispatcherActive()
			thread6=InternalResponseStageActive()
			thread7=MemtableFlushWriterActive()
			thread8=MemtablePostFlushActive()
			thread9=MemtableReclaimMemoryActive()
			thread10=MigrationStageActive()
			thread11=MiscStageActive()
			thread12=PendingRangeCalculatorActive()
			thread13=PerDiskMemtableFlushWriter_0Active()
			thread14=SamplerActive()
			thread15=SecondaryIndexManagementActive()
			thread16=ValidationExecutorActive()
			thread17=CounterMutationStageActive()
			thread18=MutationStageActive()
			thread19=ReadRepairStageActive()
			thread20=ReadStageActive()
			thread21=RequestResponseStageActive()
			thread22=ViewMutationStageActive()
			thread23=Native_Transport_RequestsActive()
			try:
				util=cpu_mem_usage(pid)
				io_info=io_usage(pid)
			except IndexError:
				print('Ending')
			ReadLatencyCount=ReadLatency()
			WriteLatencyCount=WriteLatency()
			if(thread1>0 or thread2>0 or thread3>0 or thread4>0 or thread5>0 or thread6>0 or thread7>0 or thread8>0 or thread9>0 or thread10>0 or thread11>0 or thread12>0 or thread13>0 or thread14>0 or thread15>0 or thread16>0 or thread17>0 or thread18>0 or thread19>0 or thread20>0 or thread21>0 or thread22>0 or thread23>0):
				if ReadLatencyCount>0 or WriteLatencyCount>0:
					count['count']=0
					'''ReadLatencyCount=ReadLatency()
					ReadTotalLatencyCount=ReadTotalLatency()
					WriteLatencyCount=WriteLatency()
					WriteTotalLatencyCount=WriteTotalLatency()
					if(ReadPrev['readprev']!=ReadLatencyCount and WritePrev['writeprev']!=WriteLatencyCount):'''
					output.append([str(thread1),str(thread2),str(thread3),str(thread4),str(thread5),str(thread6),str(thread7),str(thread8),str(thread9),str(thread10),str(thread11),str(thread12),str(thread13),str(thread14),str(thread15),str(thread16),str(thread17),str(thread18),str(thread19),str(thread20),str(thread21),str(thread22),str(thread23),str(cores),util[0],util[1],io_info[0],io_info[1],str(ReadLatencyCount),str(WriteLatencyCount)])
					#ReadPrev['readprev']=ReadLatencyCount
					#WritePrev['writeprev']=WriteLatencyCount
		jmxConnector.close()
