export default async function run(ctx) {
  await ctx.page.goto('data:text/html,<title>container</title>')
  await ctx.emit.item({ item_type: 'title', data: { title: await ctx.page.title() } })
}
