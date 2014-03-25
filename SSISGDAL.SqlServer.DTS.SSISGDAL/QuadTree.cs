using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SSISGDAL.SqlServer.DTS.SSISGDAL
{
    public class QuadTree<T> where T : ISpatialEnvelope
    {
        private SpatialEnvelope _envelope;
        private List<T> _records;
        private List<QuadTree<T>> _nodes;
        private int _depth;
        private int _maxDepth;


        public QuadTree(SpatialEnvelope envelope)
        {
            this._envelope = envelope;
            this._records = new List<T>();
            this._depth = 0;
            this._maxDepth = 8;
            this._nodes = new List<QuadTree<T>>();
        }

        public int depth
        {
            get { return this._depth; }
            set { this._depth = value; }
        }

        public bool add(T record)
        {
            if (!(this._envelope.contains(record.envelope)))
            {
                return false;
            }

            if (this._depth < this._maxDepth)
            {
                if (this._nodes.Count == 0) { this.subdivide(); }
                foreach (QuadTree<T> node in this._nodes)
                {
                    if (node.add(record)) { return true; }
                }
            }

            this._records.Add(record);
            return true;
        }

        public List<T> query(T record)
        {
            List<T> results = new List<T>();

            if (record.envelope.intersects(this._envelope))
            {
                if (record.envelope.contains(this._envelope))
                {
                    results.AddRange(this._records);
                }
                else
                {
                    foreach (T item in this._records)
                    {
                        if (record.envelope.intersects(item.envelope))
                        {
                            results.Add(item);
                        }
                    }
                }
                if (!(this._nodes.Count == 0))
                {
                    foreach (QuadTree<T> node in this._nodes)
                    {
                        results.AddRange(node.query(record));
                    }
                }
            }
            return results;
        }

        private void subdivide()
        {
            double width = this._envelope.maxx - this._envelope.minx; 
            double height = this._envelope.maxy - this._envelope.miny;
            double halfWidth = (width / 2);
            double halfHeight = (height / 2);

            // NW Node
            this._nodes.Insert(
                0,
                new QuadTree<T>(
                    new SpatialEnvelope(
                        this._envelope.minx,
                        this._envelope.miny + halfHeight,
                        this._envelope.minx + halfWidth,
                        this._envelope.maxy
                    )
                )
            );
            // NE Node
            this._nodes.Insert(
                1,
                new QuadTree<T>(
                    new SpatialEnvelope(
                        this._envelope.minx + halfWidth,
                        this._envelope.miny + halfHeight,
                        this._envelope.maxx,
                        this._envelope.maxy
                    )
                )
            );
            // SW Node
            this._nodes.Insert(
                2,
                new QuadTree<T>(
                    new SpatialEnvelope(
                        this._envelope.minx,
                        this._envelope.miny,
                        this._envelope.minx + halfWidth,
                        this._envelope.miny + halfHeight
                    )
                )
            );
            // SE Node
            this._nodes.Insert(
                3,
                new QuadTree<T>(
                    new SpatialEnvelope(
                        this._envelope.minx + halfWidth,
                        this._envelope.miny,
                        this._envelope.maxx,
                        this._envelope.miny + halfHeight
                    )
                )
            );

            foreach (QuadTree<T> node in this._nodes)
            {
                node.depth = this._depth + 1;
            }
        }
    }
}
