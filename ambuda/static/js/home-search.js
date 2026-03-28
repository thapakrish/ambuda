import { toHK } from './sanskrit-search';

/**
 * Alpine component for the homepage search bar with fuzzy dropdown.
 * Expects a `items` binding: array of {title, slug}.
 */
export default (items) => ({
  query: '',
  results: [],
  selectedIndex: -1,
  open: false,
  entries: [],

  init() {
    this.entries = items.map((item) => {
      const text = item.title.toLowerCase();
      const hk = toHK(item.title).toLowerCase();
      return { ...item, text, hk };
    });
  },

  search() {
    if (!this.query) {
      this.results = [];
      this.open = false;
      return;
    }
    const q = this.query.toLowerCase();
    const qHK = toHK(this.query).toLowerCase();
    this.results = this.entries
      .filter((e) => e.text.includes(q) || e.hk.includes(q) || e.text.includes(qHK) || e.hk.includes(qHK))
      .slice(0, 10);
    this.selectedIndex = -1;
    this.open = this.results.length > 0;
  },

  onKeydown(event) {
    if (!this.open) return;
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      this.selectedIndex = Math.min(this.selectedIndex + 1, this.results.length - 1);
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      this.selectedIndex = Math.max(this.selectedIndex - 1, 0);
    } else if (event.key === 'Enter') {
      event.preventDefault();
      if (this.selectedIndex >= 0) {
        this.go(this.results[this.selectedIndex].slug);
      } else if (this.results.length > 0) {
        this.go(this.results[0].slug);
      }
    } else if (event.key === 'Escape') {
      this.open = false;
    }
  },

  go(slug) {
    window.location.href = '/texts/' + slug + '/';
  },
});
