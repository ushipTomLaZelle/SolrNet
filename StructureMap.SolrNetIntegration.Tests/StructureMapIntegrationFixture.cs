using System;
using System.Collections.Generic;
using System.Configuration;
using MbUnit.Framework;
using SolrNet;
using StructureMap.SolrNetIntegration.Config;

namespace StructureMap.SolrNetIntegration.Tests {
    [TestFixture]
    [Category("Integration")]
    public class StructureMapIntegrationFixture {
        private Container _container;


        public StructureMapIntegrationFixture()
        {
            var solrConfig = (SolrConfigurationSection) ConfigurationManager.GetSection("solr");

            _container = new Container(c => c.IncludeRegistry(new SolrNetRegistry(solrConfig.SolrServers)));
        }


        ~StructureMapIntegrationFixture() {
            _container.Dispose();
        }

        [Test]
        public void DictionaryDocument() {
            var solr = _container.GetInstance<ISolrOperations<Dictionary<string, object>>>();
            var results = solr.Query(SolrQuery.All);

            Assert.GreaterThanOrEqualTo(results.Count,0);

            foreach (var d in results) {                
                Assert.GreaterThanOrEqualTo(d.Count, 0);
                foreach (var kv in d)
                    Console.WriteLine("{0}: {1}", kv.Key, kv.Value);
            }
        }

        [Test]
        public void DictionaryDocument_add() {
            var solr = _container.GetInstance<ISolrOperations<Dictionary<string, object>>>();

            solr.Add(new Dictionary<string, object> {
                {"id", "ababa"},
                {"manu", "who knows"},
                {"popularity", 55},
                {"timestamp", DateTime.UtcNow},
            });
        }

        [Test]
        public void Ping_And_Query() {
            var solr = _container.GetInstance<ISolrOperations<Entity>>();
            solr.Ping();
            Console.WriteLine(solr.Query(SolrQuery.All).Count);
        }
    }
}