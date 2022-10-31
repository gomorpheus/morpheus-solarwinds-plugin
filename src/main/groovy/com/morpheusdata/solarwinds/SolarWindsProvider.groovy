/*
* Copyright 2022 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.morpheusdata.solarwinds

import com.morpheusdata.core.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIpIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import io.reactivex.Single
import org.apache.commons.net.util.SubnetUtils
import io.reactivex.Observable

/**
 * The IPAM / DNS Provider implementation for Solarwinds IPAM
 * This contains most methods used for interacting directly with the SolarWinds SWQL API for reserving IP addresses.
 * This particular integration does not yet do anything with DNS on SolarWinds.
 * 
 * @author David Estes
 */
@Slf4j
class SolarWindsProvider implements IPAMProvider {

    MorpheusContext morpheusContext
    Plugin plugin
    final static String lockName = 'solarwinds.ipam'
    final String startIpReservationPath = "/Solarwinds/InformationService/v3/json/Invoke/IPAM.SubnetManagement/StartIpReservation"
    final String finishIpReservationPath = "/Solarwinds/InformationService/v3/json/Invoke/IPAM.SubnetManagement/FinishIpReservation"
    final String changeIpStatusPath = "/Solarwinds/InformationService/v3/json/Invoke/IPAM.SubnetManagement/ChangeIpStatus"

    final String queryPath = "/Solarwinds/InformationService/v3/json/Query"

    SolarWindsProvider(Plugin plugin, MorpheusContext morpheusContext) {
        this.morpheusContext = morpheusContext
        this.plugin = plugin
    }


    /**
     * Validation Method used to validate all inputs applied to the integration of an IPAM Provider upon save.
     * If an input fails validation or authentication information cannot be verified, Error messages should be returned
     * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
     * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
     *
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the inputs are valid or not.
     */
    @Override
    ServiceResponse verifyNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        ServiceResponse<NetworkPoolServer> rtn = ServiceResponse.error()
        rtn.data = poolServer
        HttpApiClient solarWindsClient = new HttpApiClient()
        try {
            def apiUrl = poolServer.serviceUrl
            boolean hostOnline = false
            try {
                def apiUrlObj = new URL(apiUrl)
                def apiHost = apiUrlObj.host
                def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
                hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, false, true, null)
            } catch(e) {
                log.error("Error parsing URL {}", apiUrl, e)
            }
            if(hostOnline) {
                opts.doPaging = false
                opts.maxResults = 1
                def subnetList = listSubnets(solarWindsClient,poolServer)
                if(subnetList.success) {
                    rtn.success = true
                } else {
                    rtn.msg = subnetList.msg ?: 'Error connecting to SolidServer'
                }
            } else {
                rtn.msg = 'Host not reachable'
            }
        } catch(e) {
            log.error("verifyPoolServer error: ${e}", e)
        } finally {
            solarWindsClient.shutdownClient()
        }
        return rtn
    }

    /**
     * Called during creation of a {@link NetworkPoolServer} operation. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse createNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        return null
    }

    /**
     * Called during update of an existing {@link NetworkPoolServer}. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse updateNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        return null
    }

    ServiceResponse testNetworkPoolServer(HttpApiClient client, NetworkPoolServer poolServer) {
        def rtn = new ServiceResponse()
        try {
            def networkList = listSubnets(client, poolServer)
            if(networkList.success) {
                rtn.success = true
            } else
                rtn.msg = 'error connecting to solarwinds'
            rtn.success = networkList.success
        } catch(e) {
            rtn.success = false
            log.error("test network pool server error: ${e}", e)
        }
        return rtn
    }

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching Host Records created outside of Morpheus.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     */
    @Override
    void refresh(NetworkPoolServer poolServer) {
        log.debug("refreshNetworkPoolServer: {}", poolServer.dump())
        HttpApiClient solarWindsClient = new HttpApiClient()
        solarWindsClient.throttleRate = poolServer.serviceThrottleRate
        try {
            def apiUrl = poolServer.serviceUrl
            def apiUrlObj = new URL(apiUrl)
            def apiHost = apiUrlObj.host
            def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
            def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, false, true, null)
            log.debug("online: {} - {}", apiHost, hostOnline)
            def testResults = null
            // Promise
            if(hostOnline) {
                testResults = testNetworkPoolServer(solarWindsClient,poolServer) as ServiceResponse<Map>

                if(!testResults.success) {
                    //NOTE invalidLogin was only ever set to false.
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error calling SolarWinds').blockingGet()
                } else {

                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).blockingGet()
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'SolarWinds api not reachable')
            }
            Date now = new Date()
            if(testResults?.success) {
                cacheNetworks(solarWindsClient,poolServer)
                if(poolServer?.configMap?.inventoryExisting) {
                    cacheIpAddressRecords(solarWindsClient,poolServer)
                }
                log.info("Sync Completed in ${new Date().time - now.time}ms")
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
            }
        } catch(e) {
            log.error("refreshNetworkPoolServer error: ${e}", e)
        } finally {
            solarWindsClient.shutdownClient()
        }
    }

    // cacheNetworks methods
    void cacheNetworks(HttpApiClient client, NetworkPoolServer poolServer, Map opts = [:]) {
        opts.doPaging = true
        def listResults = listSubnets(client, poolServer)
        if(listResults.success) {
            List apiItems = listResults.data?.results?.unique{it.SubnetId} as List<Map>
            Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)

            SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
            syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
                domainObject.externalId == apiItem?.SubnetId?.toString()
            }.onDelete {removeItems ->
                morpheus.network.pool.remove(poolServer.id, removeItems).blockingGet()
            }.onAdd { itemsToAdd ->
                addMissingPools(poolServer, itemsToAdd)
            }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection,Map>> updateItems ->

                Map<Long, SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return morpheus.network.pool.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPool pool ->
                    SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map> matchItem = updateItemMap[pool.id]
                    return new SyncTask.UpdateItem<NetworkPool,Map>(existingItem:pool, masterItem:matchItem.masterItem)
                }

            }.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
                updateMatchedPools(poolServer, updateItems)
            }.start()
        }
    }

    void addMissingPools(NetworkPoolServer poolServer, Collection<Map> chunkedAddList) {
        def poolType = new NetworkPoolType(code: 'solarwinds')
        List<NetworkPool> missingPoolsList = []
        chunkedAddList?.each { Map it ->
            def networkIp = it.Address && it.CIDR ? "${it.Address}/${it.CIDR}" : null
            if(networkIp) {
                def displayName = it.FriendlyName ?: networkIp
                def networkInfo = getNetworkPoolConfig(networkIp)
                def addConfig = [poolServer: poolServer,account:poolServer.account, owner:poolServer.account, name:displayName, externalId:it.SubnetId,
                                 displayName:displayName, type:poolType, cidr: networkIp, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id]
                addConfig += networkInfo.config
                def newNetworkPool = new NetworkPool(addConfig)
                newNetworkPool.ipRanges = []
                networkInfo.ranges?.each { range ->
                    log.debug("range: ${range}")
                    def rangeConfig = [networkPool:newNetworkPool, startAddress:range.startAddress, endAddress:range.endAddress, addressCount:addConfig.ipCount]
                    def addRange = new NetworkPoolRange(rangeConfig)
                    newNetworkPool.ipRanges.add(addRange)
                }
                missingPoolsList.add(newNetworkPool)
            }

        }
        morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
    }

    void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList) {
        List<NetworkPool> poolsToUpdate = []
        chunkedUpdateList?.each { update ->
            NetworkPool existingItem = update.existingItem
            if(existingItem) {
                //update view ?
                def save = false
                def name = update.masterItem.FriendlyName
                def networkIp = update.masterItem.CIDR ? "${update.masterItem.Address}/${update.masterItem.CIDR}" : null
                def displayName = name ?: networkIp
                if(existingItem?.displayName != displayName) {
                    existingItem.displayName = displayName
                    save = true
                }
                if(!existingItem.ipRanges) {
                    log.warn("no ip ranges found!")
                    def networkInfo = getNetworkPoolConfig(networkIp)
                    networkInfo.ranges?.each { range ->
                        log.info("range: ${range}")
                        def rangeConfig = [networkPool:existingItem, startAddress:range.startAddress, endAddress:range.endAddress, addressCount:networkInfo.config.ipCount]
                        def addRange = new NetworkPoolRange(rangeConfig)

                        existingItem.addToIpRanges(addRange)
                    }
                    save = true
                }
                if(existingItem.cidr != networkIp) {
                    existingItem.cidr = networkIp
                    save = true
                }
                if(save) {
                    poolsToUpdate << existingItem
                }
            }
        }
        if(poolsToUpdate.size() > 0) {
            morpheus.network.pool.save(poolsToUpdate).blockingGet()
        }
    }


    // cacheIpAddressRecords
    void cacheIpAddressRecords(HttpApiClient client, NetworkPoolServer poolServer) {
        morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).flatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
            return morpheus.network.pool.listById(poolIdents.collect{it.id})
        }.flatMap { NetworkPool pool ->
            def listResults = listIpNodes(client,poolServer,pool.externalId)


            if (listResults.success) {

                List<Map> apiItems = listResults?.data?.results?.findAll{res -> res.Status != 2} as List<Map>
                Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
                SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
                return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection ipObject, Map apiItem ->
                    ipObject.externalId == apiItem?.IPNodeId?.toString()
                }.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.ipAddress == apiItem?.IPAddress
                }.onDelete {removeItems ->
                    morpheus.network.pool.poolIp.remove(pool.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingIps(pool, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection,Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.pool.poolIp.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPoolIp poolIp ->
                        SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map> matchItem = updateItemMap[poolIp.id]
                        return new SyncTask.UpdateItem<NetworkPoolIp,Map>(existingItem:poolIp, masterItem:matchItem.masterItem)
                    }
                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedIps(updateItems)
                }.observe()
            } else {
                return Single.just(false)
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.subscribe()

    }

    void addMissingIps(NetworkPool pool, Collection<Map> addList) {

        List<NetworkPoolIp> poolIpsToAdd = addList?.collect { it ->
            def ipAddress = it.IPAddress
            def ipType = 'assigned'
            if( it.Status == 4) {
                ipType = 'reserved'
            } else if(it.Status == 8) {
                ipType = 'transient'
            }
            def addConfig = [networkPool: pool, networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null, ipType: ipType, hostname: it.DnsBackward, ipAddress: ipAddress, externalId:it.IPNodeId.toString()]
            def newObj = new NetworkPoolIp(addConfig)
            return newObj
        }
        if(poolIpsToAdd.size() > 0) {
            morpheus.network.pool.poolIp.create(pool, poolIpsToAdd).blockingGet()
        }
    }

    void updateMatchedIps(List<SyncTask.UpdateItem<NetworkPoolIp,Map>> updateList) {
        List<NetworkPoolIp> ipsToUpdate = []
        updateList?.each {  update ->
            NetworkPoolIp existingItem = update.existingItem
            if(existingItem) {
                //update view ?
                def hostname = update.masterItem.DnsBackward
                def ipType = 'assigned'
                if( update.masterItem.Status == 4) {
                    ipType = 'reserved'
                } else if(update.masterItem.Status == 8) {
                    ipType = 'transient'
                }

                def save = false
                if(existingItem.ipType != ipType) {
                    existingItem.ipType = ipType
                    save = true
                }
                if(existingItem.hostname != hostname) {
                    existingItem.hostname = hostname
                    save = true

                }
                if(save) {
                    ipsToUpdate << existingItem
                }
            }
          
        }
        if(ipsToUpdate.size() > 0) {
            morpheus.network.pool.poolIp.save(ipsToUpdate)
        }
    }


    /**
     * Called on the first save / update of a pool server integration. Used to do any initialization of a new integration
     * Often times this calls the periodic refresh method directly.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts an optional map of parameters that could be sent. This may not currently be used and can be assumed blank
     * @return a ServiceResponse containing the success state of the initialization phase
     */
    @Override
    ServiceResponse initializeNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        def rtn = new ServiceResponse()
        try {
            if(poolServer) {
                refresh(poolServer)
                rtn.data = poolServer
            } else {
                rtn.error = 'No pool server found'
            }
        } catch(e) {
            rtn.error = "initializeNetworkPoolServer error: ${e}"
            log.error("initializeNetworkPoolServer error: ${e}", e)
        }
        return rtn
    }

    /**
     * Creates a Host record on the target {@link NetworkPool} within the {@link NetworkPoolServer} integration.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp The ip address and metadata related to it for allocation. It is important to create functionality such that
     *                      if the ip address property is blank on this record, auto allocation should be performed and this object along with the new
     *                      ip address be returned in the {@link ServiceResponse}
     * @param domain The domain with which we optionally want to create an A/PTR record for during this creation process.
     * @param createARecord configures whether or not the A record is automatically created
     * @param createPtrRecord configures whether or not the PTR record is automatically created
     * @return a ServiceResponse containing the success state of the create host record operation
     */
    @Override
    ServiceResponse createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain, Boolean createARecord, Boolean createPtrRecord) {
        HttpApiClient client = new HttpApiClient()
        def lock
        try {
            lock = morpheusContext.acquireLock(lockName + ".${networkPool.id}",[timeout: 60l * 1000l]).blockingGet()
            def hostname = networkPoolIp.hostname
            if (domain && hostname && !hostname.endsWith(domain.name)) {
                hostname = "${hostname}.${domain.name}"
            }
            def cidrArgs = networkPool.cidr.tokenize('/')

            HttpApiClient.RequestOptions apiOpts = new HttpApiClient.RequestOptions(ignoreSSL: poolServer.ignoreSsl)
            apiOpts.body = JsonOutput.toJson([cidrArgs[0], cidrArgs[1], 10])
            def results
            if(!networkPoolIp.ipAddress) {
                results = client.callApi(poolServer.serviceUrl,startIpReservationPath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'POST')

                if(results.success) {
                    def ipAddress = results.content.replace('"','')
                    networkPoolIp.ipAddress = ipAddress
                    apiOpts.body = JsonOutput.toJson([ipAddress,"Used"])
                    results = client.callApi(poolServer.serviceUrl,finishIpReservationPath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'POST')
                }

            } else {
                apiOpts = new HttpApiClient.RequestOptions(queryParams: [query: "SELECT IpNodeId,IPAddress,Status FROM IPAM.IPNode WHERE IPAddress = '${networkPoolIp.ipAddress}' AND Status = 2".toString()])
                results = client.callJsonApi(poolServer.serviceUrl,queryPath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'GET')

                def dataSet = results.data.results
                if(results.success && dataSet) {
                    networkPoolIp.externalId = dataSet.first().IpNodeId.toString()
                    apiOpts.body = JsonOutput.toJson([networkPoolIp.ipAddress,'Used'])
                    results = client.callJsonApi(poolServer.serviceUrl,changeIpStatusPath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'POST')
                }else {
                    return ServiceResponse.error("Error allocating host record to the specified ip: ${networkPoolIp.ipAddress}",null,networkPoolIp)
                }
            }



            if(results?.success) {
                if(!networkPoolIp.externalId) {

                    apiOpts = new HttpApiClient.RequestOptions(queryParams: [query: "SELECT IpNodeId,IPAddress,Status FROM IPAM.IPNode WHERE IPAddress = '${networkPoolIp.ipAddress}'".toString()])

                    results = client.callJsonApi(poolServer.serviceUrl,queryPath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'GET')

                    def dataSet = results.data.results
                    if(results.success && dataSet) {
                        networkPoolIp.externalId = dataSet.first().IpNodeId.toString()
                    }
                }
                def updatePath = "/SolarWinds/InformationService/v3/Json/swis://${new URL(poolServer.serviceUrl).host}/Orion/IPAM.IPNode/IpNodeId=${networkPoolIp.externalId}"

                apiOpts = new HttpApiClient.RequestOptions(headers:['Content-Type':'application/json'],body: JsonOutput.toJson(['DNSBackward':hostname]))
                client.callApi(poolServer.serviceUrl,updatePath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'POST')

                if (networkPoolIp.id) {
                    networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
                } else {
                    networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
                }

                return ServiceResponse.success(networkPoolIp)
            } else {
                return ServiceResponse.error("Error allocating host record to the specified ip", null, networkPoolIp)
            }
        } finally {
            client.shutdownClient()
            if(lock) {
                morpheusContext.releaseLock(lockName + ".${networkPool.id}",[lock: lock]).subscribe().dispose()
            }
        }
    }

    /**
     * Updates a Host record on the target {@link NetworkPool} if supported by the Provider. If not supported, send the appropriate
     * {@link ServiceResponse} such that the user is properly informed of the unavailable operation.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp the changes to the network pool ip address that would like to be made. Most often this is just the host record name.
     * @return a ServiceResponse containing the success state of the update host record operation
     */
    @Override
    ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
        HttpApiClient client = new HttpApiClient()
        try {
            def results
            HttpApiClient.RequestOptions apiOpts
            if(networkPoolIp.ipAddress == networkPoolIp.externalId && !networkPoolIp.externalId) {
                apiOpts = new HttpApiClient.RequestOptions([
                        queryParams: [query: "SELECT IpNodeId,IPAddress,Status FROM IPAM.IPNode WHERE IPAddress = '${ipAddress}'".toString()]
                ])
                results = client.callJsonApi(poolServer.serviceUrl,queryPath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'GET')
                def dataSet = results.data.results
                if(results.success && dataSet) {
                    networkPoolIp.externalId = dataSet.first().IpNodeId.toString()
                } else {
                    return ServiceResponse.error("Unable to locate record in Solarwinds IPNode Table",null,networkPoolIp)
                }
            }

            def updatePath = "/SolarWinds/InformationService/v3/Json/swis://${new URL(poolServer.serviceUrl).host}/Orion/IPAM.IPNode/IpNodeId=${networkPoolIp.externalId}".toString()
            apiOpts = new HttpApiClient.RequestOptions([
                    headers: ['Content-Type':'application/json'],
                    body: JsonOutput.toJson(['DNSBackward':networkPoolIp.hostname])
            ])
            results = client.callApi(poolServer.serviceUrl,updatePath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, apiOpts,'POST')

            if(results?.success && results?.error != true) {
                if (networkPoolIp.id) {
                    networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
                }
                return ServiceResponse.success(networkPoolIp)
            } else {
                return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, networkPoolIp)
            }
        } finally {
            client.shutdownClient()
        }
    }

    /**
     * Deletes a host record on the target {@link NetworkPool}. This is used for cleanup or releasing of an ip address on
     * the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param poolIp the record that is being deleted.
     * @param deleteAssociatedRecords determines if associated records like A/PTR records
     * @return a ServiceResponse containing the success state of the delete operation
     */
    @Override
    ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp poolIp, Boolean deleteAssociatedRecords) {
        HttpApiClient client = new HttpApiClient()
        def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
        try {
            def results = client.callJsonApi(poolServer.serviceUrl,changeIpStatusPath,poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, new HttpApiClient.RequestOptions([body: [poolIp.ipAddress,'Available']]),'POST')
            if(results.success) {
                return ServiceResponse.success(poolIp)
            } else {
                return ServiceResponse.error(results.error ?: 'Error Deleting Host Record', null, poolIp)
            }

        } finally {
            client.shutdownClient()
        }
    }

    /**
     * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
     * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
     */
    @Override
    Collection<NetworkPoolType> getNetworkPoolTypes() {
        return [
                new NetworkPoolType(code:'solarwinds', name:'SolarWinds', creatable:false, description:'SolarWinds', rangeSupportsCidr: false)
        ]
    }

    /**
     * Provide custom configuration options when creating a new {@link AccountIntegration}
     * @return a List of OptionType
     */
    @Override
    List<OptionType> getIntegrationOptionTypes() {
        return [
                new OptionType(code: 'networkPoolServer.solarwindsipam.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', placeHolder: 'https://x.x.x.x:17778', helpBlock: 'gomorpheus.help.serviceUrl', displayOrder: 0),
                new OptionType(code: 'networkPoolServer.solarwindsipam.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local',optionSource: 'credentials',config: '{"credentialTypes":["username-password"]}'),
                new OptionType(code: 'networkPoolServer.solarwindsipam.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2, localCredential: true),
                new OptionType(code: 'networkPoolServer.solarwindsipam.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3, localCredential: true),
                new OptionType(code: 'networkPoolServer.solarwindsipam.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 4),
                new OptionType(code: 'networkPoolServer.solarwindsipam.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 5),
                new OptionType(code: 'networkPoolServer.solarwindsipam.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 6)
        ]
    }

    /**
     * Returns the IPAM Integration logo for display when a user needs to view or add this integration
     * @since 0.12.3
     * @return Icon representation of assets stored in the src/assets of the project.
     */
    @Override
    Icon getIcon() {
        return new Icon(path:"solarwinds-black.svg", darkPath: "solarwinds-white.svg")
    }

    /**
     * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
     *
     * @return an implementation of the MorpheusContext for running Future based rxJava queries
     */
    @Override
    MorpheusContext getMorpheus() {
        return morpheusContext
    }

    /**
     * Returns the instance of the Plugin class that this provider is loaded from
     * @return Plugin class contains references to other providers
     */
    @Override
    Plugin getPlugin() {
        return plugin
    }

    /**
     * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
     * that is seeded or generated related to this provider will reference it by this code.
     * @return short code string that should be unique across all other plugin implementations.
     */
    @Override
    String getCode() {
        return "solarwindsipam"
    }

    /**
     * Provides the provider name for reference when adding to the Morpheus Orchestrator
     * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
     *
     * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
     */
    @Override
    String getName() {
        return "SolarWinds IPAM"
    }


    ServiceResponse listSubnets(HttpApiClient client, NetworkPoolServer poolServer) {
//        def apiOpts = [
//                query: [query: 'SELECT SubnetId,ParentId,FriendlyName,Address,CIDR FROM IPAM.Subnet'],
//                noRedirects: opts.noRedirects
//        ]
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: poolServer.ignoreSsl)
        requestOptions.queryParams = [query: 'SELECT SubnetId,ParentId,FriendlyName,Address,CIDR FROM IPAM.Subnet']
        def results = client.callJsonApi(poolServer.serviceUrl, queryPath, poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, requestOptions, 'GET')
        return results
    }


    ServiceResponse listIpNodes(HttpApiClient client, NetworkPoolServer poolServer, subnetId) {
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: poolServer.ignoreSsl)
        requestOptions.queryParams =  [query: "SELECT SubnetId,IPNodeId,DnsBackward,IPAddress,Status FROM IPAM.IPNode WHERE SubnetId=${subnetId}".toString()]
        def results = client.callJsonApi(poolServer.serviceUrl, queryPath, poolServer.credentialData?.username as String  ?: poolServer.serviceUsername, poolServer.credentialData?.password as String  ?: poolServer.servicePassword, requestOptions, 'GET')
        return results
    }

    private static Map getNetworkPoolConfig(cidr) {
        def rtn = [config:[:], ranges:[]]
        try {
            def subnetInfo = new SubnetUtils(cidr).getInfo()
            rtn.config.netmask = subnetInfo.getNetmask()
            rtn.config.ipCount = subnetInfo.getAddressCountLong() ?: 0
            rtn.config.ipFreeCount = rtn.config.ipCount
            rtn.ranges << [startAddress:subnetInfo.getLowAddress(), endAddress:subnetInfo.getHighAddress()]
        } catch(e) {
            log.warn("error parsing network pool cidr: ${e}", e)
        }
        return rtn
    }
}
