package org.fcrepo.fixity.resources;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.fcrepo.AbstractResource;
import org.fcrepo.services.LowLevelStorageService;
import org.fcrepo.services.ObjectService;
import org.fcrepo.utils.LowLevelCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/fixity-corrupter")
public class FixityCorrupter extends AbstractResource{

	private static final Logger logger = LoggerFactory.getLogger(FixityCorrupter.class);

    @Inject
    private ObjectService objects;

    @Inject
    private LowLevelStorageService llstorage;

	@GET
	@Path("/{path: .*}/{numCorrupt}")
	public Response corruptDatastream(@PathParam("path") final String path, @PathParam("numCorrupt") int numCorrupt) throws RepositoryException, IOException {

	    final Set<LowLevelCacheEntry> cacheEntries = this.llstorage.getLowLevelCacheEntries(objects.getObjectNode(getAuthenticatedSession(),path));

	    /* iterate over all the lowlevel cacheentries until numCorrupt reaches 0 */
	    for (Iterator<LowLevelCacheEntry> entryIterator = cacheEntries.iterator();entryIterator.hasNext();){
	        if (numCorrupt-- <= 0) {
	            break;
	        }
	        LowLevelCacheEntry entry = entryIterator.next();
            byte[] corrupted = new byte[4096];
            new Random().nextBytes(corrupted);
            logger.debug("corrupting " + path + " instance: " + entry.getExternalId());
            ByteArrayInputStream src = new ByteArrayInputStream(corrupted);
            entry.storeValue(src);
	    }

		return Response.ok().build();
	}

}
