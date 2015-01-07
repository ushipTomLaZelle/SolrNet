using System.Collections.Generic;
using System.Configuration;
using MbUnit.Framework;

using SolrNet;
using SolrNet.Exceptions;
using SolrNet.Impl;
using StructureMap.SolrNetIntegration.Config;

namespace StructureMap.SolrNetIntegration.Tests
{
    [TestFixture]
    public class StructureMapFixture
    {
        private Container _container;


        public StructureMapFixture()
        {
            var solrConfig = (SolrConfigurationSection)ConfigurationManager.GetSection("solr");

            _container = new Container(c => c.IncludeRegistry(new SolrNetRegistry(solrConfig.SolrServers)));
        }

        [Test]
        public void Cache()
        {

            _container.Configure(cfg => cfg.For<ISolrCache>().Use<HttpRuntimeCache>());
            var connectionId = "entity" + typeof(SolrConnection);
            var connection = (SolrConnection)_container.GetInstance<ISolrConnection>(connectionId);
            Assert.IsNotNull(connection.Cache);

            Assert.IsInstanceOfType<HttpRuntimeCache>(connection.Cache);
        }

        [Test]
        public void Container_has_ISolrDocumentPropertyVisitor()
        {

            _container.GetInstance<ISolrDocumentPropertyVisitor>();
        }

        [Test]
        public void Container_has_ISolrFieldParser()
        {

            var parser = _container.GetInstance<ISolrFieldParser>();
            Assert.IsNotNull(parser);
        }

        [Test]
        public void Container_has_ISolrFieldSerializer()
        {

            _container.GetInstance<ISolrFieldSerializer>();
        }

        [Test]
        public void DictionaryDocument_and_multi_core()
        {
            var cores = new SolrServers {
                new SolrServerElement {
                    Id = "default",
                    DocumentType = typeof (Entity).AssemblyQualifiedName,
                    Url = "http://localhost:8983/solr/entity1",
                },
                new SolrServerElement {
                    Id = "entity1dict",
                    DocumentType = typeof (Dictionary<string, object>).AssemblyQualifiedName,
                    Url = "http://localhost:8983/solr/entity1",
                },
                new SolrServerElement {
                    Id = "another",
                    DocumentType = typeof (Entity2).AssemblyQualifiedName,
                    Url = "http://localhost:8983/solr/entity2",
                },
            };
            var container = new Container(c => c.IncludeRegistry(new SolrNetRegistry(cores)));
            var solr1 = container.GetInstance<ISolrOperations<Entity>>();
            var solr2 = container.GetInstance<ISolrOperations<Entity2>>();
            var solrDict = container.GetInstance<ISolrOperations<Dictionary<string, object>>>();
        }

        [Test]
        public void DictionaryDocument_ResponseParser()
        {

            var parser = _container.GetInstance<ISolrDocumentResponseParser<Dictionary<string, object>>>();
            Assert.IsInstanceOfType<SolrDictionaryDocumentResponseParser>(parser);
        }

        [Test]
        public void DictionaryDocument_Serializer()
        {

            var serializer = _container.GetInstance<ISolrDocumentSerializer<Dictionary<string, object>>>();

            Assert.IsInstanceOfType<SolrDictionarySerializer>(serializer);
        }

        [Test]
        public void RegistersSolrConnectionWithAppConfigServerUrl()
        {

            var instanceKey = "entity" + typeof(SolrConnection);

            var solrConnection = (SolrConnection)_container.GetInstance<ISolrConnection>(instanceKey);

            Assert.AreEqual("http://afitzgerald:8081/solr/collection1", solrConnection.ServerURL);
        }

        [Test]
        public void ResolveSolrOperations()
        {

            var m = _container.GetInstance<ISolrOperations<Entity>>();
            Assert.IsNotNull(m);
        }

        [Test, ExpectedException(typeof(InvalidURLException))]
        public void Should_throw_exception_for_invalid_protocol_on_url()
        {
            var solrServers = new SolrServers {
                new SolrServerElement {
                    Id = "test",
                    Url = "htp://localhost:8893",
                    DocumentType = typeof (Entity2).AssemblyQualifiedName,
                }
            };
            var container = new Container(c => c.IncludeRegistry(new SolrNetRegistry(solrServers)));
            container.GetInstance<SolrConnection>();
        }

        [Test, ExpectedException(typeof(InvalidURLException))]
        public void Should_throw_exception_for_invalid_url()
        {
            var solrServers = new SolrServers {
                new SolrServerElement {
                    Id = "test",
                    Url = "http:/localhost:8893",
                    DocumentType = typeof (Entity2).AssemblyQualifiedName,
                }
            };
            var container = new Container(c => c.IncludeRegistry(new SolrNetRegistry(solrServers)));
            container.GetInstance<SolrConnection>();
        }
    }
}