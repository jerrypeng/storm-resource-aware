/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SupervisorDetails {

    String id;
    /**
     * hostname of this supervisor
     */
    String host;
    Object meta;
    /**
     * meta data configured for this supervisor
     */
    Object schedulerMeta;
    /**
     * all the ports of the supervisor
     */
    Set<Integer> allPorts;

    private Map<String, Number> _total_resources;

    public SupervisorDetails(String id, String host, Object meta, Object schedulerMeta, 
          Collection<Number> allPorts, Map<String, Number> total_resources){
      
        this.id = id;
        this.host = host;
        this.meta = meta;
        this.schedulerMeta = schedulerMeta;
        if(allPorts!=null) {
          setAllPorts(allPorts);
        } else {
          this.allPorts = new HashSet();
        }
        this._total_resources = total_resources;
    }
    public SupervisorDetails(String id, Object meta){
        this(id, null,meta, null, null, null);
    }

    public SupervisorDetails(String id, Object meta, Map<String, Number> total_resources){
        this(id, null, meta, null, null, total_resources);
    }
    
    public SupervisorDetails(String id, Object meta, Collection<Number> allPorts){
        this(id, null, meta, null, allPorts, null);
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta, Collection<Number> allPorts){
        this(id, host, null, schedulerMeta, allPorts, null);
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta,
        Collection<Number> allPorts, Map<String, Number> total_resources){
        this(id, host, null, schedulerMeta, allPorts, total_resources);
    }

    private void setAllPorts(Collection<Number> allPorts) {
        this.allPorts = new HashSet<Integer>();
        if(allPorts!=null) {
            for(Number n: allPorts) {
                this.allPorts.add(n.intValue());
            }
        }
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public Object getMeta() {
        return meta;
    }
    
    public Set<Integer> getAllPorts() {
        return allPorts;
    }

    public Object getSchedulerMeta() {
        return this.schedulerMeta;
    }

    private Double getTotalResource(String type) {
        if (this._total_resources == null) {
          return null;
        } else {
            if (this._total_resources.containsKey(type) == false) {
                return null;
            } else {
                Number totalResource = this._total_resources.get(type);
                return totalResource.doubleValue();
            }
        }
    }

    public Double getTotalMemory() {
        return this.getTotalResource(Globals.TYPE_MEMORY);
    }

    public Double getTotalCPU() {
      return this.getTotalResource(Globals.TYPE_CPU);
    }
}
