<script>
  import { createEventDispatcher, onMount } from "svelte";
  import * as echarts from "echarts/core";
  import { SunburstChart as EchartsSunburstChart } from "echarts/charts";
  import { GraphicComponent, TooltipComponent } from "echarts/components";
  import { CanvasRenderer } from "echarts/renderers";

  echarts.use([EchartsSunburstChart, TooltipComponent, GraphicComponent, CanvasRenderer]);

  export let rows = [];
  export let lens = "uncached_input";
  export let lensLabel = "Uncached input";
  export let rootLabel = "Current scope";
  export let selectedKey = null;
  export let selectedRow = null;
  export let childRowsByParent = {};
  export let loading = false;

  const dispatch = createEventDispatcher();
  const palette = {
    Project: "#c05621",
    ActionCategory: "#dd8a55",
    Action: "#a73919",
    Directory: "#6f5a4b",
    File: "#9f8a78",
  };

  let container;
  let chart;
  let resizeObserver;

  function metricValue(row) {
    return Number(row?.metrics?.[lens] ?? 0);
  }

  function itemColor(row) {
    return palette[row.kind] ?? "#8d6b55";
  }

  function childColor(childRow) {
    return palette[childRow.kind] ?? "#8d6b55";
  }

  function mutedColor(hex, alpha = 0.18) {
    const color = hex.replace("#", "");
    if (color.length !== 6) {
      return `rgba(141, 107, 85, ${alpha})`;
    }

    const r = Number.parseInt(color.slice(0, 2), 16);
    const g = Number.parseInt(color.slice(2, 4), 16);
    const b = Number.parseInt(color.slice(4, 6), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }

  function formatMetric(value) {
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Number(value ?? 0));
  }

  function totalValue() {
    return rows.reduce((sum, row) => sum + metricValue(row), 0);
  }

  function visibleChildCount() {
    return Object.values(childRowsByParent).reduce((sum, childRows) => sum + childRows.length, 0);
  }

  function supportsDrill(row) {
    return rows.some((candidate) => candidate.key === row?.key) || Boolean(row?.full_path);
  }

  function parentLabel(row) {
    const value = metricValue(row);
    const label = value >= totalValue() * 0.08 || row.key === selectedKey ? row.label : "";
    return {
      show: Boolean(label),
      formatter: label,
      color: "#20150d",
      fontWeight: row.key === selectedKey ? 700 : 600,
    };
  }

  function childLabel(childRow) {
    const value = metricValue(childRow);
    const label = value >= totalValue() * 0.035 || childRow.key === selectedKey ? childRow.label : "";
    return {
      show: Boolean(label),
      formatter: label,
      color: "#3b271a",
      fontWeight: childRow.key === selectedKey ? 700 : 500,
    };
  }

  function chartData() {
    return {
      name: rootLabel,
      value: totalValue(),
      itemStyle: {
        color: "rgba(192, 86, 33, 0.12)",
        borderColor: "rgba(255, 248, 239, 0.96)",
      },
      label: {
        show: false,
      },
      children: rows.map((row) => {
        const childRows = childRowsByParent[row.key] ?? [];
        const selectedParent = row.key === selectedKey;
        return {
          name: row.label,
          value: Math.max(metricValue(row), 0.01),
          rowKey: row.key,
          row,
          itemStyle: {
            color: itemColor(row),
            borderColor: selectedParent ? "#20150d" : "rgba(255, 248, 239, 0.92)",
            borderWidth: selectedParent ? 3 : 1,
            shadowBlur: selectedParent ? 18 : 0,
            shadowColor: selectedParent ? "rgba(32, 21, 13, 0.22)" : "transparent",
            opacity: selectedKey && !selectedParent ? 0.84 : 0.95,
          },
          label: parentLabel(row),
          emphasis: {
            itemStyle: {
              shadowBlur: 22,
              shadowColor: "rgba(32, 21, 13, 0.26)",
            },
          },
          children: childRows.map((childRow) => ({
            name: childRow.label,
            value: Math.max(metricValue(childRow), 0.01),
            rowKey: childRow.key,
            row: childRow,
            itemStyle: {
              color:
                childRow.key === selectedKey
                  ? childColor(childRow)
                  : mutedColor(childColor(childRow), 0.22),
              borderColor:
                childRow.key === selectedKey ? "#20150d" : "rgba(255, 248, 239, 0.88)",
              borderWidth: childRow.key === selectedKey ? 3 : 1,
              opacity: childRow.key === selectedKey ? 1 : selectedKey ? 0.7 : 0.84,
            },
            label: childLabel(childRow),
          })),
        };
      }),
    };
  }

  function centerGraphic() {
    const label = selectedRow?.label ?? rootLabel;
    const detail = selectedRow
      ? `${formatMetric(metricValue(selectedRow))} ${lensLabel.toLowerCase()}`
      : `${formatMetric(totalValue())} ${lensLabel.toLowerCase()}`;
    const subdetail = selectedRow?.kind ?? `${rows.length} parents · ${visibleChildCount()} children`;

    return [
      {
        type: "text",
        left: "center",
        top: "39%",
        silent: true,
        style: {
          text: label,
          fontSize: 18,
          fontWeight: 700,
          fontFamily: "Fraunces, Georgia, serif",
          fill: "#20150d",
          textAlign: "center",
          width: 200,
          overflow: "truncate",
        },
      },
      {
        type: "text",
        left: "center",
        top: "47%",
        silent: true,
        style: {
          text: detail,
          fontSize: 13,
          fontWeight: 700,
          fontFamily: "IBM Plex Mono, monospace",
          fill: "#8a4a21",
          textAlign: "center",
        },
      },
      {
        type: "text",
        left: "center",
        top: "53%",
        silent: true,
        style: {
          text: subdetail,
          fontSize: 12,
          fontFamily: "Source Sans 3, sans-serif",
          fill: "#665244",
          textAlign: "center",
        },
      },
      {
        type: "text",
        left: "center",
        top: "58%",
        silent: true,
        style: {
          text: loading ? "mapping child rings..." : "click to select · double-click to drill",
          fontSize: 11,
          fontFamily: "IBM Plex Mono, monospace",
          fill: loading ? "#8a4a21" : "#8a7768",
          textAlign: "center",
        },
      },
    ];
  }

  function updateChart() {
    if (!chart) {
      return;
    }

    chart.setOption(
      {
        animationDuration: 280,
        animationDurationUpdate: 280,
        graphic: centerGraphic(),
        series: [
          {
            type: "sunburst",
            nodeClick: false,
            radius: ["16%", "92%"],
            sort: (a, b) => b.getValue() - a.getValue(),
            data: [chartData()],
            levels: [
              {},
              {
                r0: "16%",
                r: "42%",
                label: {
                  rotate: 0,
                  minAngle: 14,
                  overflow: "truncate",
                },
              },
              {
                r0: "42%",
                r: "66%",
                label: {
                  rotate: "radial",
                  minAngle: 10,
                  overflow: "truncate",
                },
              },
              {
                r0: "66%",
                r: "92%",
                label: {
                  rotate: "radial",
                  minAngle: 9,
                  overflow: "truncate",
                },
              },
            ],
            emphasis: {
              focus: "ancestor",
            },
            itemStyle: {
              borderRadius: 10,
            },
            labelLayout: {
              hideOverlap: true,
            },
            breadcrumb: {
              show: false,
            },
          },
        ],
        tooltip: {
          trigger: "item",
          backgroundColor: "rgba(32, 21, 13, 0.9)",
          borderWidth: 0,
          padding: [10, 12],
          textStyle: {
            color: "#fff8ee",
          },
          formatter: (params) => {
            const row = params.data.row;
            if (!row) {
              return [
                `<strong>${rootLabel}</strong>`,
                `${formatMetric(totalValue())} ${lensLabel.toLowerCase()}`,
                `${rows.length} visible parents · ${visibleChildCount()} visible children`,
              ].join("<br />");
            }
            const opportunityCount = row.opportunities?.annotations?.length ?? 0;
            return [
              `<strong>${row.label}</strong>`,
              `${row.kind} · ${formatMetric(metricValue(row))} ${lensLabel.toLowerCase()}`,
              supportsDrill(row) ? "can drill deeper" : "leaf row in current hierarchy",
              opportunityCount > 0 ? `${opportunityCount} opportunity annotations` : "no opportunity annotations",
              row.full_path ?? "",
              "click to select · double-click to drill",
            ]
              .filter(Boolean)
              .join("<br />");
          },
        },
      },
      true,
    );
  }

  onMount(() => {
    chart = echarts.init(container, null, { renderer: "canvas" });
    chart.on("click", (params) => {
      if (params.data?.rowKey) {
        dispatch("select", { row: params.data.row });
      }
    });
    chart.on("dblclick", (params) => {
      if (params.data?.rowKey) {
        dispatch("drill", { row: params.data.row });
      }
    });

    resizeObserver = new ResizeObserver(() => {
      chart?.resize();
    });
    resizeObserver.observe(container);
    updateChart();

    return () => {
      resizeObserver?.disconnect();
      chart?.dispose();
    };
  });

  $: if (chart) {
    updateChart();
  }
</script>

{#if rows.length}
  <div class:loading class="sunburst-shell">
    <div bind:this={container} class="sunburst-chart" aria-label="Sunburst chart"></div>
    {#if loading}
      <div class="sunburst-loading">Mapping deeper rings for the current view.</div>
    {/if}
  </div>
{:else}
  <div class="sunburst-empty">No rows available for the current scope.</div>
{/if}

<style>
  .sunburst-shell {
    position: relative;
  }

  .sunburst-chart {
    width: 100%;
    min-height: 32rem;
  }

  .sunburst-shell.loading .sunburst-chart {
    opacity: 0.92;
  }

  .sunburst-loading {
    position: absolute;
    right: 1rem;
    bottom: 1rem;
    padding: 0.45rem 0.7rem;
    border-radius: 999px;
    background: rgba(32, 21, 13, 0.8);
    color: #fff8ee;
    font-family: "IBM Plex Mono", ui-monospace, monospace;
    font-size: 0.72rem;
    letter-spacing: 0.04em;
  }

  .sunburst-empty {
    min-height: 18rem;
    display: grid;
    place-items: center;
    color: #665244;
    background: rgba(255, 255, 255, 0.7);
    border-radius: 1rem;
  }
</style>
