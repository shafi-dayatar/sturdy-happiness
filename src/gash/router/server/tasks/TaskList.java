/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server.tasks;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.db.ChunkRow;
import pipe.work.Work.Task;

/**
 * Processing of tasks
 * 
 * @author gash
 *
 */
public class TaskList {
	protected static Logger logger = LoggerFactory.getLogger("work");

	private LinkedBlockingDeque<Task> inbound;
	private int processed;
	private int balanced;
	private Rebalancer rebalance;

	public TaskList(Rebalancer rb) {
		rebalance = rb;
	}

	public void addTask(Task t) {
		inbound.add(t);
	}

	public int numEnqueued() {
		return inbound.size();
	}

	public int numProcessed() {
		return processed;
	}

	public int numBalanced() {
		return balanced;
	}

	/**
	 * task taken to be given to another node
	 * 
	 * @return
	 */
	public Task rebalance() {
		Task t = null;

		try {
			if (rebalance != null && !rebalance.allow())
				return t;

			t = inbound.take();
			balanced++;
		} catch (InterruptedException e) {
			logger.error("failed to rebalance a task", e);
		}
		return t;
	}

	/**
	 * task taken to be processed by this node
	 * 
	 * @return
	 */
	public Task dequeue() {
		Task t = null;
		//dequeue only if queue is big. BIG = 20
		//TODO: Make 'BIG' dynamic , remove below hardcoding


		try {
			if(inbound.size() > 20){
				t = inbound.take();
				processed++;
			}
		} catch (InterruptedException e) {
			logger.error("failed to dequeue a task", e);
		}
		return t;
	}

	public synchronized Task getStealableTask(ServerState state, int node_id){

		Iterator<Task> taskIterator = inbound.iterator();
		Task task = null;
		while (taskIterator.hasNext()){
			task = taskIterator.next();
			String filename = task.getMsg().getReq().getRrb().getFilename();
			ChunkRow[] arr = state.getDb().getChunkRows(filename);
			for(ChunkRow r : arr){
				logger.info(" checkinbg for " + r.getLocation_at() + " message req filename  " + filename);
				if(r.getLocation_at().contains(Integer.toString(node_id))){
					logger.info(" node_id " + node_id + " will steal for "+ filename + " of type " + task.getMsg().getReq().getRequestType());
					//yes the node can steal this task now

					//remove task from queue
					inbound.remove(task);
					return task;
				}
			}

		}
		return task;
	}
}
