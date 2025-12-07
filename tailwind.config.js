module.exports = {
  content: [
    './ambuda/static/js/*.js',
    './ambuda/static/js/*.ts',
    './ambuda/templates/**/*.html',
    './ambuda/utils/parse_alignment.py',
    './ambuda/utils/xml.py',
    './ambuda/views/proofing/main.py',
  ],
  safelist: [
    // ProseMirror HTML tag highlighting
    // TODO: still needed? we can probably delete these.
    "pm-html-tag",
    "pm-html-tag-error",
  ],
  plugins: [
    require('@tailwindcss/typography')({
      className: 'tw-prose',
    }),
  ]
}
