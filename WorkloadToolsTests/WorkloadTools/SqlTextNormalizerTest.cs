using Microsoft.VisualStudio.TestTools.UnitTesting;
using WorkloadTools.Consumer.Analysis;

namespace WorkloadToolsTests.WorkloadTools
{
    [TestClass]
    public class SqlTextNormalizerTest
    {
        private SqlTextNormalizer _normalizer;

        [TestInitialize]
        public void Initialize()
        {
            _normalizer = new SqlTextNormalizer();
        }

        [TestMethod]
        public void NormalizeSqlText_SimpleStringParam_Normalized()
        {
            var sql = "exec SampleStoredProcedure @Param1=N'Name1',@Key=N'123456'";
            var result = _normalizer.NormalizeSqlText(sql, 1, false);
            Assert.IsNotNull(result);
            StringAssert.Contains(result.NormalizedText, "@PARAM1 = {STR}");
            StringAssert.Contains(result.NormalizedText, "@KEY = {STR}");
        }

        [TestMethod]
        public void NormalizeSqlText_StringParamWithDoubleQuotes_Normalized()
        {
            var sql = "exec SampleStoredProcedure @Param1=N'Name1',@Key=N'123456',@Content=N'<ROOT Attribute1=\"value\" xmlns=\"http://namespace.com\">value</ROOT>'";
            var result = _normalizer.NormalizeSqlText(sql, 1, false);
            Assert.IsNotNull(result);
            StringAssert.Contains(result.NormalizedText, "@PARAM1 = {STR}");
            StringAssert.Contains(result.NormalizedText, "@KEY = {STR}");
            StringAssert.Contains(result.NormalizedText, "@CONTENT = {STR}");
            Assert.IsFalse(result.NormalizedText.Contains("ATTRIBUTE1"), "The XML attribute content should have been replaced by {STR}");
        }

        [TestMethod]
        public void NormalizeSqlText_StringParamWithEscapedSingleQuotes_Normalized()
        {
            var sql = "exec SampleStoredProcedure @Param1=N'It''s a test'";
            var result = _normalizer.NormalizeSqlText(sql, 1, false);
            Assert.IsNotNull(result);
            StringAssert.Contains(result.NormalizedText, "@PARAM1 = {STR}");
        }
    }
}
