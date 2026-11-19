import { Fragment, type ReactNode } from "react";

const INLINE = /(\*\*[^*]+\*\*|`[^`]+`|\[[^\]]+\]\(https?:\/\/[^)\s]+\))/g;

function inline(text: string): ReactNode[] {
  return text.split(INLINE).map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**") && part.length > 4) return <strong key={i}>{part.slice(2, -2)}</strong>;
    if (part.startsWith("`") && part.endsWith("`") && part.length > 2) return <code key={i}>{part.slice(1, -1)}</code>;
    const link = part.match(/^\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)$/);
    if (link)
      return (
        <a key={i} href={link[2]} target="_blank" rel="noreferrer">
          {link[1]}
        </a>
      );
    return <Fragment key={i}>{part}</Fragment>;
  });
}

function block(lines: string[], key: number): ReactNode {
  if (lines.every((l) => /^\s*[-*] /.test(l)))
    return (
      <ul key={key}>
        {lines.map((l, i) => (
          <li key={i}>{inline(l.replace(/^\s*[-*] /, ""))}</li>
        ))}
      </ul>
    );
  if (lines.every((l) => /^\s*\d+\. /.test(l)))
    return (
      <ol key={key}>
        {lines.map((l, i) => (
          <li key={i}>{inline(l.replace(/^\s*\d+\. /, ""))}</li>
        ))}
      </ol>
    );
  if (lines.length >= 2 && lines.every((l) => l.trim().startsWith("|"))) {
    const rows = lines.filter((l) => !/^\s*\|[\s:|-]+\|\s*$/.test(l)).map((l) => l.trim().replace(/^\||\|$/g, "").split("|").map((c) => c.trim()));
    return (
      <table key={key}>
        <tbody>
          {rows.map((row, r) => (
            <tr key={r}>
              {row.map((cell, c) => (r === 0 ? <th key={c}>{inline(cell)}</th> : <td key={c}>{inline(cell)}</td>))}
            </tr>
          ))}
        </tbody>
      </table>
    );
  }
  const heading = lines.length === 1 && lines[0].match(/^#{1,4}\s+(.*)$/);
  if (heading) return <p key={key} className="md-heading">{inline(heading[1])}</p>;
  return (
    <p key={key}>
      {lines.map((l, i) => (
        <Fragment key={i}>
          {i > 0 && <br />}
          {inline(l)}
        </Fragment>
      ))}
    </p>
  );
}

export function Markdown({ text }: { text: string }) {
  const parts = text.split(/```[a-zA-Z0-9]*\n?/);
  const nodes: ReactNode[] = [];
  parts.forEach((part, i) => {
    if (i % 2 === 1) {
      nodes.push(
        <pre key={`c${i}`} className="md-code">
          {part.replace(/\n$/, "")}
        </pre>,
      );
      return;
    }
    let current: string[] = [];
    const flush = () => {
      if (current.length) nodes.push(block(current, nodes.length));
      current = [];
    };
    for (const line of part.split("\n")) {
      if (!line.trim()) flush();
      else if (current.length && /^\s*([-*]|\d+\.) /.test(line) !== /^\s*([-*]|\d+\.) /.test(current[0])) {
        flush();
        current.push(line);
      } else current.push(line);
    }
    flush();
  });
  return <div className="md">{nodes}</div>;
}
