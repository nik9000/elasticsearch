package org.elasticsearch.client.real;

import org.elasticsearch.client.real.request.IndexRequest;

/**
 * Official Java client for Elasticsearch.
 */
public class Client {
    private final Admin admin = new Admin();

    /**
     * The admin client that can be used to perform administrative operations.
     */
    public Admin admin() {
        return admin;
    }

    /**
     * Index a document associated with a given index and type.
     * <p/>
     * <p>The id is optional, if it is not provided, one will be generated automatically.
     */
    public IndexRequest index() {
        return new IndexRequest();
    }

    /**
     * The admin client that can be used to perform administrative operations.
     */
    public class Admin {
        private ClusterAdminClient cluster = new ClusterAdminClient();
        private IndicesAdminClient indices = new IndicesAdminClient();
        /**
         * A client allowing to perform actions/operations against the cluster.
         */
        public ClusterAdminClient cluster() {
            return cluster;
        }

        /**
         * A client allowing to perform actions/operations against the indices.
         */
        public IndicesAdminClient indices() {
            return indices;
        }
    }
}
