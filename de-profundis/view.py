class MainView(App):
    async def on_load(self, event: events.Load) -> None:
        await self.bind("b", "toggle_sidebar", "Toggle sidebar")
        await self.bind("p", "pause", "Pause")
        await self.bind("r", "reload", "Reload rspreadsheet")
        await self.bind("q", "quit", "Quit")
    
    show_bar = Reactive(False)

    def watch_show_bar(self, show_bar: bool) -> None:
        self.bar.animate("layout_offset_x", 0 if show_bar else -40)

    def action_toggle_sidebar(self) -> None:
        self.show_bar = not self.show_bar

    async def on_mount(self, event: events.Mount) -> None:
        # A scrollview to contain the markdown file
        self.header = Header()
        self.footer = Footer()
        self.bar = Placeholder()
        self.body_table = ScrollView(gutter=1)
        self.body_console = ScrollView(gutter=1)

        # Header / footer / dock
        await self.view.dock(
            self.header, 
            edge="top",
        )
        await self.view.dock(
            self.footer, 
            edge="bottom",
        )
        await self.view.dock(
            self.bar,
            edge="left", 
            size=40,
            name="sidebar",
        )

        # Dock the body in the remaining space
        await self.view.dock(
            self.body_table, 
            self.body_console,
            edge="top",
        )

        # Table update callback
        async def get_table() -> None:
            await self.body_table.update(self.update_table())

        # Console update callback
        async def get_console(console) -> None:
            with Live(self.body_console, refresh_per_second=4) as live:  # update 4 times a second to feel fluid       
                await self.body_console.update(update_console(self.body_console))

        await self.call_later(get_table)
        await self.call_later(get_console)


    def update_table(
        self,
    ) -> Table:
        table = Table(
            title=f"Current state of the datasets",
            show_lines=True,
        )

        table.add_column(header="Project", footer="Project", justify="left", style="green", no_wrap=True)
        table.add_column(header="Task", footer="Task", justify="left", style="cyan")
        table.add_column(header="Name", footer="Name", justify="left", style="blue bold")
        table.add_column(header="Status", footer="Status", justify="left", style="magenta")
        table.add_column(header="Total size", footer="Total size", justify="left", style="magenta")
        table.add_column(header="Is manual", footer="Is manual", justify="left", style="magenta")
        table.add_column(header="# of URLs", footer="# of URLs", justify="left", style="magenta")

        for _, dataset in df.iterrows():
            table.add_row(
                dataset["Project"],
                dataset["Task"],
                dataset["Dataset name"],
                dataset["Status"],
                str(dataset["Total size"]),
                TRUTH[dataset["is_manual"]],
                str(len(dataset["URL(s)"])),
            )

        return table


    def update_console(
        self,
        console,
    ):
        console.bell()
        console.print(f"Working on row #{row}")

        return console
