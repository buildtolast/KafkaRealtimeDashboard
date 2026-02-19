import { useState, useEffect } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import { useMessageTracking } from '../contexts/MessageTrackingContext';

const TOPIC_COLORS = [
  '#48aff0', '#3dcc91', '#ff6b6b', '#ffd93d', '#c084fc',
  '#ff9f43', '#0abde3', '#ff6b9d', '#a3e635', '#fb923c',
];

interface ChartViewProps {
  topics: string[];
}

export function ChartView({ topics }: ChartViewProps) {
  const tracking = useMessageTracking();
  const [chartData, setChartData] = useState<Record<string, any>[]>([]);

  useEffect(() => {
    function refresh() {
      if (!tracking) return;
      const history = tracking.getHistory();
      const data = history.map(({ time, counts }) => {
        const point: Record<string, any> = {
          time: new Date(time * 1000).toLocaleTimeString(),
        };
        topics.forEach((t) => {
          point[t] = counts[t] || 0;
        });
        return point;
      });
      setChartData(data);
    }
    refresh();
    const interval = setInterval(refresh, 2000);
    return () => clearInterval(interval);
  }, [tracking, topics]);

  if (chartData.length === 0) {
    return (
      <div className="chart-view" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#5c7080' }}>
        Collecting message rate data... (updates every 5 seconds)
      </div>
    );
  }

  return (
    <div className="chart-view">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData} margin={{ top: 20, right: 30, left: 10, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#394b59" />
          <XAxis dataKey="time" stroke="#a7b6c2" fontSize={11} />
          <YAxis stroke="#a7b6c2" fontSize={11} allowDecimals={false} />
          <Tooltip
            contentStyle={{
              background: '#252a31',
              border: '1px solid #394b59',
              borderRadius: 4,
              fontSize: 12,
            }}
            labelStyle={{ color: '#f6f7f9' }}
          />
          <Legend
            wrapperStyle={{ fontSize: 12 }}
          />
          {topics.map((topic, i) => (
            <Line
              key={topic}
              type="monotone"
              dataKey={topic}
              stroke={TOPIC_COLORS[i % TOPIC_COLORS.length]}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
