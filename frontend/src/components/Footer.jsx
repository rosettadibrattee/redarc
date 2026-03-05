export default function Footer() {
  return (
    <footer className="text-center py-6 mt-auto border-t border-border-subtle text-[11px] text-text-tertiary">
      <span>RedarcX - Self-Hostable Reddit Archive</span>
      {' · '}
      <a
        href="https://github.com/rosettadibrattee/redarc"
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-accent transition-colors"
      >
        GitHub Project
      </a>
      {' · '}
      <a
        href="https://github.com/Yakabuff/redarc"
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-accent transition-colors"
      >
        Original Yakabuff/redarc
      </a>
      {' · '}
      <a
        href="http://opensource.org/licenses/MIT"
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-accent transition-colors"
      >
        MIT License
      </a>
    </footer>
  );
}
