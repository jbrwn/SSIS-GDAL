using System;
using System.Collections.Generic;
using System.Text;
using OSGeo.OGR;

namespace SSISGDAL.SqlServer.DTS.SSISGDAL
{
    public class OGRBufferCacheRow : ISpatialEnvelope
    {
        private SpatialEnvelope _envelope;
        private Geometry _geom;
        private object[] _bufferRow;

        public OGRBufferCacheRow(object[] bufferRow, Geometry geom)
        {
            this._bufferRow = bufferRow;
            this._geom = geom;
            this._envelope = this.calculateEnvelope(geom);
        }

        public object this[int index]
        {
            get { return this._bufferRow[index]; }
        }

        public Geometry geometry
        {
            get { return this._geom; }
        }

        public SpatialEnvelope envelope
        {
            get { return this._envelope; }
        }

        private SpatialEnvelope calculateEnvelope(Geometry geom)
        {
            Envelope env = new Envelope();
            geom.GetEnvelope(env);
            SpatialEnvelope ogrenv = new SpatialEnvelope(env.MinX, env.MinY, env.MaxX, env.MaxY);
            return ogrenv;
        }

    }
}
