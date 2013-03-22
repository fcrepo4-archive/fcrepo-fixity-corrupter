package org.fcrepo.fixity.resources;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import javax.inject.Inject;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.fcrepo.FedoraObject;
import org.fcrepo.services.LowLevelStorageService;
import org.fcrepo.services.ObjectService;
import org.fcrepo.utils.LowLevelCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class FixityCorrupter {

	private static final Logger logger = LoggerFactory.getLogger(FixityCorrupter.class);

    @Inject
    private ObjectService objectService;

	@GET
	@Path("{pid}/{dsId}/{num}")
	public Response corruptDatastreams(@PathParam("pid") final String pid, @PathParam("dsId") final String dsId, @DefaultValue("-1") @PathParam("num") int numCorrupt) throws RepositoryException, IOException {
		final FedoraObject fo = objectService.getObject(pid);
		NodeIterator streams = fo.getNode().getNodes();
		final Random rnd = new Random();
		while (streams.hasNext()){
			Node ds = (Node) streams.next();
			if (!ds.getName().equals(dsId)){
				continue;
			}
			Iterator<Entry<LowLevelCacheEntry,InputStream>> blobs = LowLevelStorageService.getBinaryBlobs(ds).entrySet().iterator();
			while (blobs.hasNext()){
				if (numCorrupt-- == 0){
					/* enough mayhem by user request */
					return Response.ok().build();
				}
				Entry<LowLevelCacheEntry,InputStream> blob = blobs.next();
				byte[] corrupted = new byte[4096];
				rnd.nextBytes(corrupted);
				logger.debug("corrupting " + ds.getName() + " instance: " + blob.getKey());
				ByteArrayInputStream in = new ByteArrayInputStream(corrupted);
				blob.getKey().storeValue(in);
			}
		}
		return Response.ok().build();
	}
}
