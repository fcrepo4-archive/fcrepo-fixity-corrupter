/**
 * Copyright 2013 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.fixity.resources;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.fcrepo.AbstractResource;
import org.fcrepo.services.LowLevelStorageService;
import org.fcrepo.services.ObjectService;
import org.fcrepo.session.InjectedSession;
import org.fcrepo.utils.LowLevelCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@Path("/fixity-corrupter")
public class FixityCorrupter extends AbstractResource {

    private static final Logger logger = LoggerFactory
            .getLogger(FixityCorrupter.class);

    @InjectedSession
    protected Session session;

    @Inject
    private ObjectService objects;

    @Inject
    private LowLevelStorageService llstorage;

    /**
     * TODO
     * 
     * @param path
     * @return
     * @throws RepositoryException
     * @throws IOException
     */
    @POST
    @Path("/{path}")
    public Response corruptDatastream(
            @PathParam("path")
            final String path) throws RepositoryException, IOException {
        int numCorrupt = 1;

        final Set<LowLevelCacheEntry> cacheEntries =
                this.llstorage.getLowLevelCacheEntries(objects.getObjectNode(
                        session, path));

        /* iterate over all lowlevel cacheentries until numCorrupt reaches 0 */
        for (Iterator<LowLevelCacheEntry> entryIterator =
                cacheEntries.iterator(); entryIterator.hasNext();) {
            if (numCorrupt-- <= 0) {
                break;
            }
            LowLevelCacheEntry entry = entryIterator.next();
            byte[] corrupted = new byte[4096];
            new Random().nextBytes(corrupted);
            logger.debug("corrupting " + path + " instance: " +
                    entry.getExternalId());
            ByteArrayInputStream src = new ByteArrayInputStream(corrupted);
            entry.storeValue(src);
        }

        return Response.ok().build();
    }

}
