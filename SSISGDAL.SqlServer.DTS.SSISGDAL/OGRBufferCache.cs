using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using OSGeo.OGR;


namespace SSISGDAL.SqlServer.DTS.SSISGDAL
{
    public class OGRBufferCache : IEnumerable
    {
        private List<OGRBufferCacheRow> _rows;
        private SpatialEnvelope _envelope;
        private QuadTree<OGRBufferCacheRow> _spatialIndex;

        public OGRBufferCache()
        {
            this._rows = new List<OGRBufferCacheRow>();
            this._envelope = null;
            this._spatialIndex = null;
        }

        public int count
        {
            get { return this._rows.Count; }
        }

        public void createSpatialIndex()
        {
            this._spatialIndex = new QuadTree<OGRBufferCacheRow>(this._envelope);
            foreach (OGRBufferCacheRow row in this._rows)
            {
                this._spatialIndex.add(row);
            }
        }

        public void add(OGRBufferCacheRow row)
        {
            this.setEnvelope(row.envelope);
            this._rows.Add(row);
            if (this._spatialIndex != null)
            {
                this._spatialIndex.add(row);
            }
        }

        public List<OGRBufferCacheRow> intersects(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> candidateRows = this.getCandidates(other);
            List<OGRBufferCacheRow> returnRows = new List<OGRBufferCacheRow>();
            
            foreach (OGRBufferCacheRow row in candidateRows)
            {
                if (row.geometry.Intersects(other.geometry))
                {
                    returnRows.Add(row);
                }
            }
            return returnRows;
        }

        public List<OGRBufferCacheRow> equals(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> candidateRows = this.getCandidates(other);
            List<OGRBufferCacheRow> returnRows = new List<OGRBufferCacheRow>();

            foreach (OGRBufferCacheRow row in candidateRows)
            {
                if (row.geometry.Equal(other.geometry))
                {
                    returnRows.Add(row);
                }
            }
            return returnRows;
        }

        //public List<OGRBufferCacheRow> disjoint(OGRBufferCacheRow other)
        //{
        //}

        public List<OGRBufferCacheRow> touches(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> candidateRows = this.getCandidates(other);
            List<OGRBufferCacheRow> returnRows = new List<OGRBufferCacheRow>();

            foreach (OGRBufferCacheRow row in candidateRows)
            {
                if (row.geometry.Touches(other.geometry))
                {
                    returnRows.Add(row);
                }
            }
            return returnRows;
        }

        public List<OGRBufferCacheRow> crosses(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> candidateRows = this.getCandidates(other);
            List<OGRBufferCacheRow> returnRows = new List<OGRBufferCacheRow>();

            foreach (OGRBufferCacheRow row in candidateRows)
            {
                if (row.geometry.Crosses(other.geometry))
                {
                    returnRows.Add(row);
                }
            }
            return returnRows;
        }

        public List<OGRBufferCacheRow> within(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> candidateRows = this.getCandidates(other);
            List<OGRBufferCacheRow> returnRows = new List<OGRBufferCacheRow>();

            foreach (OGRBufferCacheRow row in candidateRows)
            {
                if (row.geometry.Within(other.geometry))
                {
                    returnRows.Add(row);
                }
            }
            return returnRows;
        }

        public List<OGRBufferCacheRow> contains(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> candidateRows = this.getCandidates(other);
            List<OGRBufferCacheRow> returnRows = new List<OGRBufferCacheRow>();

            foreach (OGRBufferCacheRow row in candidateRows)
            {
                if (row.geometry.Contains(other.geometry))
                {
                    returnRows.Add(row);
                }
            }
            return returnRows;
        }

        public List<OGRBufferCacheRow> overlaps(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> candidateRows = this.getCandidates(other);
            List<OGRBufferCacheRow> returnRows = new List<OGRBufferCacheRow>();

            foreach (OGRBufferCacheRow row in candidateRows)
            {
                if (row.geometry.Overlaps(other.geometry))
                {
                    returnRows.Add(row);
                }
            }
            return returnRows;
        }

        private void setEnvelope(SpatialEnvelope other)
        {
            if (this._envelope == null)
            {
                this._envelope = other;
            }
            else
            {
                if (other.maxx > this._envelope.maxx) { this._envelope.maxx = other.maxx; }
                if (other.maxy > this._envelope.maxy) { this._envelope.maxy = other.maxy; }
                if (other.minx < this._envelope.minx) { this._envelope.minx = other.minx; }
                if (other.miny < this._envelope.miny) { this._envelope.miny = other.miny; }
            }
        }

        private List<OGRBufferCacheRow> getCandidates(OGRBufferCacheRow other)
        {
            List<OGRBufferCacheRow> rows;
            if (this._spatialIndex != null)
            {
                rows = this._spatialIndex.query(other);
            }
            else
            {
                rows = this._rows;
            }
            return rows;
        }

        public IEnumerator GetEnumerator()
        {
            foreach (OGRBufferCacheRow row in this._rows)
            {
                yield return row;
            }
        }

    }
}
