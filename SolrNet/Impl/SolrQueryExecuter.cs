﻿#region license
// Copyright (c) 2007-2009 Mauricio Scheffer
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#endregion

using System;
using System.Collections.Generic;
using SolrNet.Commands.Parameters;
using SolrNet.Utils;

namespace SolrNet.Impl {
    /// <summary>
    /// Executes queries
    /// </summary>
    /// <typeparam name="T">Document type</typeparam>
    public class SolrQueryExecuter<T> : ISolrQueryExecuter<T> where T : new() {

        private readonly ISolrQueryResultParser<T> resultParser;
        private readonly ISolrConnection connection;

        /// <summary>
        /// When the row count is not defined, use this row count by default
        /// </summary>
        public int DefaultRows { get; set; }

        public static readonly int ConstDefaultRows = 100000000;

        public SolrQueryExecuter(ISolrConnection connection, ISolrQueryResultParser<T> resultParser) {
            this.connection = connection;
            this.resultParser = resultParser;
            DefaultRows = ConstDefaultRows;
        }

        public KeyValuePair<T1, T2> KVP<T1, T2>(T1 a, T2 b) {
            return new KeyValuePair<T1, T2>(a, b);
        }

        /// <summary>
        /// Gets Solr parameters for all defined query options
        /// </summary>
        /// <param name="Query"></param>
        /// <param name="Options"></param>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<string, string>> GetAllParameters(ISolrQuery Query, QueryOptions Options) {
            var param = new List<KeyValuePair<string, string>> {
                KVP("q", Query.Query)
            };
            if (Options != null) {
                if (Options.Start.HasValue)
                    param.Add(KVP("start", Options.Start.ToString()));
                var rows = Options.Rows.HasValue ? Options.Rows.Value : DefaultRows;
                param.Add(KVP("rows", rows.ToString()));
                if (Options.OrderBy != null && Options.OrderBy.Count > 0) {
                    param.Add(KVP("sort", Func.Join(",", Options.OrderBy)));
                }

                if (Options.Fields != null && Options.Fields.Count > 0)
                    param.Add(KVP("fl", Func.Join(",", Options.Fields)));

                if (Options.FacetQueries != null && Options.FacetQueries.Count > 0) {
                    param.Add(KVP("facet", "true"));
                    foreach (var fq in Options.FacetQueries) {
                        foreach (var fqv in fq.Query) {
                            param.Add(fqv);
                        }
                    }
                }

                foreach (var p in GetHighlightingParameters(Options)) {
                    param.Add(p);
                }

                param.AddRange(GetFilterQueries(Options));
                param.AddRange(GetSpellCheckingParameters(Options));
                param.AddRange(GetMoreLikeThisParameters(Options));
                if (Options.ExtraParams != null)
                    param.AddRange(Options.ExtraParams);
            }

            return param;
        }

        /// <summary>
        /// Gets Solr parameters for defined more-like-this options
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<string, string>> GetMoreLikeThisParameters(QueryOptions options) {
            if (options.MoreLikeThis == null)
                yield break;
            var mlt = options.MoreLikeThis;
            yield return KVP("mlt", "true");
            if (mlt.Fields != null)
                yield return KVP("mlt.fl", Func.Join(",", mlt.Fields));
            if (mlt.Boost.HasValue)
                yield return KVP("mlt.boost", mlt.Boost.ToString().ToLowerInvariant());
            if (mlt.Count.HasValue)
                yield return KVP("mlt.count", mlt.Count.ToString());
            if (mlt.MaxQueryTerms.HasValue)
                yield return KVP("mlt.maxqt", mlt.MaxQueryTerms.ToString());
            if (mlt.MaxTokens.HasValue)
                yield return KVP("mlt.maxntp", mlt.MaxTokens.ToString());
            if (mlt.MaxWordLength.HasValue)
                yield return KVP("mlt.maxwl", mlt.MaxWordLength.ToString());
            if (mlt.MinDocFreq.HasValue)
                yield return KVP("mlt.mindf", mlt.MinDocFreq.ToString());
            if (mlt.MinTermFreq.HasValue)
                yield return KVP("mlt.mintf", mlt.MinTermFreq.ToString());
            if (mlt.MinWordLength.HasValue)
                yield return KVP("mlt.minwl", mlt.MinWordLength.ToString());
            if (mlt.QueryFields != null && mlt.QueryFields.Count > 0)
                yield return KVP("mlt.qf", Func.Join(",", mlt.QueryFields));
        }

        /// <summary>
        /// Gets Solr parameters for defined filter queries
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<string, string>> GetFilterQueries(QueryOptions options) {
            if (options.FilterQueries == null || options.FilterQueries.Count == 0)
                yield break;
            foreach (var fq in options.FilterQueries) {
                yield return new KeyValuePair<string, string>("fq", fq.Query);
            }
        }

        /// <summary>
        /// Gets Solr parameters for defined highlightings
        /// </summary>
        /// <param name="Options"></param>
        /// <returns></returns>
        public IDictionary<string, string> GetHighlightingParameters(QueryOptions Options) {
            var param = new Dictionary<string, string>();
            if (Options.Highlight != null) {
                var h = Options.Highlight;
                param["hl"] = "true";
                if (h.Fields != null) {
                    param["hl.fl"] = Func.Join(",", h.Fields);

                    if (h.Snippets.HasValue)
                        param["hl.snippets"] = h.Snippets.Value.ToString();

                    if (h.Fragsize.HasValue)
                        param["hl.fragsize"] = h.Fragsize.Value.ToString();

                    if (h.RequireFieldMatch.HasValue)
                        param["hl.requireFieldMatch"] = h.RequireFieldMatch.Value.ToString().ToLowerInvariant();

                    if (h.AlternateField != null)
                        param["hl.alternateField"] = h.AlternateField;

                    if (h.BeforeTerm != null)
                        param["hl.simple.pre"] = h.BeforeTerm;

                    if (h.AfterTerm != null)
                        param["hl.simple.post"] = h.AfterTerm;

                    if (h.RegexSlop.HasValue)
                        param["hl.regex.slop"] = h.RegexSlop.Value.ToString();

                    if (h.RegexPattern != null)
                        param["hl.regex.pattern"] = h.RegexPattern;

                    if (h.RegexMaxAnalyzedChars.HasValue)
                        param["hl.regex.maxAnalyzedChars"] = h.RegexMaxAnalyzedChars.Value.ToString();
                }
            }
            return param;
        }

        /// <summary>
        /// Gets solr parameters for defined spell-checking
        /// </summary>
        /// <param name="Options"></param>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<string, string>> GetSpellCheckingParameters(QueryOptions Options) {
            var spellCheck = Options.SpellCheck;
            if (spellCheck != null) {
                yield return KVP("spellcheck", "true");
                if (!string.IsNullOrEmpty(spellCheck.Query))
                    yield return KVP("spellcheck.q", spellCheck.Query);
                if (spellCheck.Build.HasValue)
                    yield return KVP("spellcheck.build", spellCheck.Build.ToString().ToLowerInvariant());
                if (spellCheck.Collate.HasValue)
                    yield return KVP("spellcheck.collate", spellCheck.Collate.ToString().ToLowerInvariant());
                if (spellCheck.Count.HasValue)
                    yield return KVP("spellcheck.count", spellCheck.Count.ToString());
                if (!string.IsNullOrEmpty(spellCheck.Dictionary))
                    yield return KVP("spellcheck.dictionary", spellCheck.Dictionary);
                if (spellCheck.OnlyMorePopular.HasValue)
                    yield return KVP("spellcheck.onlyMorePopular", spellCheck.OnlyMorePopular.ToString().ToLowerInvariant());
                if (spellCheck.Reload.HasValue)
                    yield return KVP("spellcheck.reload", spellCheck.Reload.ToString().ToLowerInvariant());
            }
        }

        /// <summary>
        /// Executes the query and returns results
        /// </summary>
        /// <returns>query results</returns>
        public ISolrQueryResults<T> Execute(ISolrQuery q, QueryOptions options) {
            var param = GetAllParameters(q, options);
            string r = connection.Get("/select", param);
            var qr = resultParser.Parse(r);
            return qr;
        }
    }
}