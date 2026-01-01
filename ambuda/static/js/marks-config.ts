export interface InlineMarkConfig {
  name: string;
  emoji: string;
  label: string;
  className: string;
  excludes?: string;
}

export const INLINE_MARKS: InlineMarkConfig[] = [
  {
    name: 'error',
    emoji: '⛔',
    label: 'Mark as error',
    className: 'pm-error',
    excludes: 'fix',
  },
  {
    name: 'fix',
    emoji: '✅',
    label: 'Mark as fix',
    className: 'pm-fix',
    excludes: 'error',
  },
  {
    name: 'flag',
    emoji: '?',
    label: 'Mark as unclear',
    className: 'pm-flag',
  },
  {
    name: 'ref',
    emoji: 'ref: ',
    label: 'Mark as footnote number',
    className: 'pm-ref',
  },
  {
    name: 'stage',
    emoji: '🎬',
    label: 'Mark as stage direction',
    className: 'pm-stage',
    excludes: 'speaker',
  },
  {
    name: 'speaker',
    emoji: '📣',
    label: 'Mark as speaker',
    className: 'pm-speaker',
    excludes: 'stage',
  },
];

export type MarkName = typeof INLINE_MARKS[number]['name'];

export function getAllMarkNames(): string[] {
  return INLINE_MARKS.map(m => m.name);
}
