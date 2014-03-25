using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SSISGDAL.SqlServer.DTS.SSISGDAL
{
    public class SpatialEnvelope
    {
        private double _minx;
        private double _miny;
        private double _maxx;
        private double _maxy;

        public SpatialEnvelope() 
        { 
        }
        
        public SpatialEnvelope(double minx, double miny, double maxx, double maxy)
        {
            this._minx = minx;
            this._miny = miny;
            this._maxx = maxx;
            this._maxy = maxy;
        }

        public double minx 
        { 
            get { return this._minx; }
            set { this._minx = value; }
        }

        public double miny 
        { 
            get { return this._miny; }
            set { this._miny = value; }
        }

        public double maxx 
        {
            get { return this._maxx; }
            set { this._maxx = value; }
        }

        public double maxy 
        { 
            get { return this._maxy; }
            set { this._maxy = value; }
        }

        public bool intersects(SpatialEnvelope other)
        {
            if (this._minx <= other.maxx &&
                this._maxx >= other.minx &&
                this._miny <= other.maxy &&
                this._maxy >= other.miny)
            {
                return true;
            }
            return false;
        }

        public bool contains(SpatialEnvelope other)
        {
            if (this._minx <= other.minx &&
                this._maxx >= other.maxx &&
                this._miny <= other.miny &&
                this._maxy >= other.maxy)
            {
                return true;
            }
            return false;
        }
    }
}
