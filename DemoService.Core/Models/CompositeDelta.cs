using System;
using System.Collections.Generic;
using System.Text;

namespace DemoService.Core.Models
{
    public class CompositeDelta : IDelta
    {
        private IDictionary<IComparable, ICollection<IDelta>> namedDeltas;

        public CompositeDelta(IDictionary<IComparable, ICollection<IDelta>> deltas)
        {
            if (deltas == null)
                throw new ArgumentException();
            this.namedDeltas = deltas;
        }

        public CompositeDelta(IDictionary<string, IDelta> deltas)
        {
            if (deltas == null)
                throw new ArgumentException();
            this.namedDeltas = new Dictionary<IComparable, ICollection<IDelta>>();
            foreach (var kvp in deltas)
            {
                this.namedDeltas.Add(kvp.Key, new List<IDelta>(new IDelta[] { kvp.Value }));
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            foreach (var key in namedDeltas.Keys)
            {
                sb.AppendLine($"{key}:\n\t{string.Join("\n\t", string.Join("\n\t", namedDeltas[key]))}");
            }

            return sb.ToString();
        }

        public bool WithDifference()
        {
            return namedDeltas != null && namedDeltas.Count > 0;
        }
    }
}
